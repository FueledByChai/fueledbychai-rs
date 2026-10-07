//! A hand seed never arms a live market (decision 0067, augmenting 0055; the owner's answer C
//! to RB-olg-3, Reviewer B's RB80-10 on PR #80, DeepSeek's DS-3 on PR #86).
//!
//! A market whose position the consumer seeded by hand ([`Registry::seed_position`]) is refused
//! Start, Flatten and Wind-down, changing nothing, unless the registry was built for a declared
//! owner-assisted testnet run ([`Registry::for_testnet_run`]); a later resync only compares its
//! position, so it stays refused. A market seeded by a trustworthy resync arms as before, with
//! or without the declaration, and the declaration never arms a market whose position is
//! unknown.

#[path = "common/arm.rs"]
mod arm;
mod common;

use arm::{leases, named, seed};
use common::lots;
use fbc_core::{InstrumentId, MonoNs, SignedLots, WallNs};
use fbc_oms::{
    ArmRefusal, EntryState, LadderConfig, Leases, MarketCapsConfig, MarketEntry, OrderKey,
    PreTradeCaps, Registry, ResyncSnapshot, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);

type Arm = fn(&mut Registry, InstrumentId, Leases) -> Result<MarketEntry, ArmRefusal>;

fn arming_calls() -> [(&'static str, Arm); 3] {
    [
        ("start", Registry::start),
        ("flatten", Registry::flatten),
        ("wind-down", Registry::wind_down),
    ]
}

fn caps() -> PreTradeCaps {
    PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(50)),
                resting: Some(lots(1_000)),
            },
        )
        .unwrap()
}

/// A named registry, not declared a testnet run.
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

/// Calls `call` on `INST` with its leases and asserts it refused with `why`, changing nothing.
fn refused(reg: &mut Registry, name: &str, call: Arm, why: ArmRefusal) {
    let before = seen(reg.entry(INST));
    let now = leases(reg, INST);
    assert_eq!(call(reg, INST, now), Err(why), "{name}");
    assert_eq!(seen(reg.entry(INST)), before, "{name}");
    assert_eq!(before, (false, EntryState::CancelOnly, 0), "{name}");
}

/// Calls `call` on `INST` with its leases and asserts it armed the market.
fn arms(reg: &mut Registry, name: &str, call: Arm) {
    let now = leases(reg, INST);
    assert!(call(reg, INST, now).unwrap().armed(), "{name}");
}

#[test]
fn a_market_seeded_by_hand_is_refused_for_live_arming_changing_nothing() {
    for (name, call) in arming_calls() {
        let mut reg = live();
        assert!(!reg.testnet_run());
        reg.seed_position(INST, SignedLots(20)).unwrap();
        // The position is known (the caps use it), but no trustworthy resync seeded it.
        assert_eq!(reg.position(INST), Some(SignedLots(20)));
        refused(&mut reg, name, call, ArmRefusal::SeededByHand(INST));
    }
}

#[test]
fn a_later_resync_that_agrees_leaves_a_hand_seeded_market_refused() {
    for (name, call) in arming_calls() {
        let mut reg = live();
        reg.seed_position(INST, SignedLots(20)).unwrap();
        // A later resync only compares the seed: it never seeds the market again.
        let report = reg
            .resync(
                &LadderConfig::new(
                    std::time::Duration::from_secs(1),
                    std::time::Duration::ZERO,
                    std::time::Duration::from_secs(10),
                    1,
                )
                .unwrap(),
                &common::order_caps(),
                &ResyncSnapshot {
                    watermark: WallNs(1_000),
                    requested_at: MonoNs(1_000),
                    orders: vec![],
                    positions: vec![(INST, SignedLots(20))],
                },
                OrderKey {
                    venue: None,
                    ingest: 1,
                },
            )
            .unwrap();
        assert!(report.seeded.is_empty(), "{name}");
        assert_eq!(report.checks.len(), 1, "{name}");
        refused(&mut reg, name, call, ArmRefusal::SeededByHand(INST));
    }
}

#[test]
fn a_market_seeded_by_a_trustworthy_resync_is_still_armable() {
    for (name, call) in arming_calls() {
        let mut reg = live();
        seed(&mut reg, &[(INST, 20)]);
        assert_eq!(reg.position(INST), Some(SignedLots(20)));
        arms(&mut reg, name, call);
    }
    // The declaration changes nothing for a market a trustworthy resync seeded.
    for (name, call) in arming_calls() {
        let mut reg = testnet();
        seed(&mut reg, &[(INST, 20)]);
        arms(&mut reg, name, call);
    }
}

#[test]
fn a_declared_testnet_run_seeds_by_hand_and_arms() {
    for (name, call) in arming_calls() {
        let mut reg = testnet();
        assert!(reg.testnet_run());
        reg.seed_position(INST, SignedLots(-3)).unwrap();
        arms(&mut reg, name, call);
        assert_eq!(reg.position(INST), Some(SignedLots(-3)), "{name}");
    }
}

#[test]
fn a_declared_testnet_run_never_arms_a_market_whose_position_is_unknown() {
    for (name, call) in arming_calls() {
        let mut reg = testnet();
        refused(&mut reg, name, call, ArmRefusal::PositionUnknown(INST));
    }
}

#[test]
fn a_killed_hand_seeded_market_is_refused_as_killed_first() {
    for (name, call) in arming_calls() {
        let mut reg = live();
        reg.seed_position(INST, SignedLots(0)).unwrap();
        reg.kill(INST);
        let now = leases(&reg, INST);
        assert_eq!(
            call(&mut reg, INST, now),
            Err(ArmRefusal::Killed(INST)),
            "{name}"
        );
    }
}

#[test]
fn the_refusal_names_the_market_and_the_way_out() {
    let text = ArmRefusal::SeededByHand(INST).to_string();
    assert!(text.contains("by hand"), "{text}");
    assert!(text.contains("testnet"), "{text}");
}
