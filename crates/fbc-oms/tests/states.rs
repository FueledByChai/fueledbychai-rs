//! Each market's order-entry state and its armed flag (decision 0012; 0013 rules 1 and 3).
//!
//! For a market in Killed and in Cancel-only, no place, amend, replace or batch item is built,
//! reduce-only, I6-reducing, flatten, force-close and wind-down ones included, and the
//! refusal comes before either cap is consulted; cancels and cancel-many are still built.
//! Lifting the kill switch while armed leaves the market in Cancel-only, building nothing. A
//! fresh registry has every market disarmed and in Cancel-only. Flatten or Wind-down on a
//! disarmed market arms it straight into Exit; a disarm leaves Cancel-only; only Start reaches
//! Quoting. Start, Flatten and Wind-down are refused, changing nothing, without the market
//! lease, without the account lease where the nonce scope is per account, and before the
//! first trustworthy resync. No resync or ladder timer moves a market, and every change
//! advances its state generation once.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the resting cap is set past any order here, since
//! the refusals judged are the state's, not the caps'.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use arm::{account_lease, lease_keys, leases, market_lease, named, start, wire};
use common::{cid, lease_dir, lots, order_caps, placement, symbol, vid};
use fbc_core::{
    AckLevel, AmendAck, AmendCaps, AmendQty, CancelBatch, ClientOrderId, InstrumentId, ItemRef,
    MarketLease, MonoNs, NewOrder, NonceScope, OrderCaps, OrderKind, RefKind, Side, SignedLots,
    SnapshotSource, SubmitOutcome, TagSet, Ticks, Tif, VenueCommand, WallNs,
};
use fbc_oms::{
    AmendRefusal, ArmRefusal, CancelChoice, EntryState, ExitKind, LadderConfig, LeaseKeys, Leases,
    MarketCapsConfig, MarketEntry, OmsError, OrderKey, OrderOp, PreTradeCaps, Registry,
    ResyncSnapshot, StateRefusal,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// The position each scenario holds: long 20.
const LONG: i64 = 20;

fn caps() -> PreTradeCaps {
    [INST, OTHER]
        .into_iter()
        .try_fold(PreTradeCaps::new(), |caps, m| {
            caps.with_market(
                m,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(1_000_000)),
                },
            )
        })
        .unwrap()
}

/// A named registry under the caps, its markets' positions not yet known.
fn unseeded() -> Registry {
    named(Registry::with_caps(caps()))
}

/// A named registry under the caps, `INST` and `OTHER` seeded long `LONG` and flat.
fn seeded() -> Registry {
    let mut reg = unseeded();
    reg.seed_position(INST, SignedLots(LONG)).unwrap();
    reg.seed_position(OTHER, SignedLots(0)).unwrap();
    reg
}

fn entry(armed: bool, state: EntryState, generation: u64) -> (bool, EntryState, u64) {
    (armed, state, generation)
}

fn seen(e: MarketEntry) -> (bool, EntryState, u64) {
    (e.armed(), e.state(), e.generation().get())
}

fn sell(qty: i64, px: i64) -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(px) },
        ..placement(cid(), px, qty)
    }
}

fn reduce_only(order: NewOrder) -> NewOrder {
    NewOrder {
        reduce_only: true,
        reducing: true,
        ..order
    }
}

/// Every kind of place 0012 names, on the long position: an ordinary buy, a reduce-only sell,
/// a sell I6 admits because it reduces the position, and the reduce-only exits a flatten, a
/// force-close (a crossing IOC) and a wind-down (a resting maker exit) would send.
fn places() -> Vec<(&'static str, NewOrder)> {
    vec![
        ("ordinary", placement(cid(), 100, 5)),
        ("reduce-only", reduce_only(sell(5, 101))),
        ("I6-reducing", sell(10, 101)),
        ("flatten", reduce_only(sell(15, 101))),
        (
            "force-close",
            NewOrder {
                tif: Tif::Ioc,
                post_only: false,
                ..reduce_only(sell(15, 90))
            },
        ),
        ("wind-down", reduce_only(sell(15, 102))),
    ]
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// Places `order` (admitted) and acknowledges it under the venue id `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    reg.place(order).unwrap();
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid(v)),
    };
    reg.on_outcome(c, OrderOp::Place, &item, &accepted(), MonoNs(1))
        .unwrap();
    c
}

/// Amends on a venue whose amend keeps the venue id, or, for a replace, gives a new one;
/// single and batch cancels by venue id.
fn venue(replace: bool) -> OrderCaps {
    OrderCaps {
        amend: Some(AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id: !replace,
            ack: AmendAck::ReplacedEvent,
            qty_semantics: AmendQty::TotalIncludingFilled,
            keeps_priority: None,
        }),
        batch_cancel: Some(CancelBatch {
            max_items: 10,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..order_caps()
    }
}

/// A market armed and Quoting, long `LONG`, resting a buy of 5 at 100 and a sell of 5 at 101.
fn quoting() -> (Registry, ClientOrderId, ClientOrderId) {
    let mut reg = seeded();
    start(&mut reg, INST);
    let bid = open(&mut reg, placement(cid(), 100, 5), "v-bid");
    let ask = open(&mut reg, sell(5, 101), "v-ask");
    (reg, bid, ask)
}

/// Each amend and replace of the two resting orders the scenario tries: the bid raised, the
/// ask lowered to the touch as a reducing exit, each on a venue that keeps the venue id and on
/// one that gives a new one.
fn amends() -> Vec<(&'static str, bool, bool, Ticks, i64, bool)> {
    // (name, replace, the bid (else the ask), price, total, reducing)
    vec![
        ("amend ordinary", false, true, Ticks(100), 6, false),
        ("amend reducing", false, false, Ticks(100), 5, true),
        ("replace ordinary", true, true, Ticks(99), 6, false),
        ("replace reducing", true, false, Ticks(100), 4, true),
    ]
}

fn try_amend(
    reg: &mut Registry,
    (bid, ask): (ClientOrderId, ClientOrderId),
    (_, replace, on_bid, px, qty, reducing): (&str, bool, bool, Ticks, i64, bool),
) -> Result<VenueCommand, AmendRefusal> {
    let c = if on_bid { bid } else { ask };
    reg.live(c)
        .unwrap()
        .amend(&venue(replace), px, lots(qty), reducing)
        .map(|cmd| cmd.command().clone())
}

#[test]
fn every_command_the_scenario_tries_is_built_while_quoting() {
    // The control: every place and amend refused below is admitted by both caps in Quoting,
    // so only the market's state refuses it there.
    for (name, order) in places() {
        let (mut reg, _, _) = quoting();
        assert!(reg.place(order).is_ok(), "{name}");
    }
    let (mut reg, _, _) = quoting();
    let plan = reg
        .place_batch(places().into_iter().map(|(_, o)| o).collect())
        .unwrap();
    assert!(plan.refused.is_empty(), "{:?}", plan.refused);
    for amend in amends() {
        let (mut reg, bid, ask) = quoting();
        assert!(
            try_amend(&mut reg, (bid, ask), amend).is_ok(),
            "{}",
            amend.0
        );
    }
}

#[test]
fn killed_and_cancel_only_build_no_place_amend_replace_or_batch_item_but_build_cancels() {
    // Killed armed, Killed disarmed, Cancel-only armed (the kill switch lifted) and Cancel-only
    // disarmed, each with the same resting orders and position as the control.
    type Into = fn(&mut Registry);
    let cases: [(&str, Into, StateRefusal); 4] = [
        (
            "killed, armed",
            |reg| {
                reg.kill(INST);
            },
            StateRefusal::Killed(INST),
        ),
        (
            "killed, disarmed",
            |reg| {
                reg.kill(INST);
                reg.disarm(INST);
            },
            StateRefusal::Killed(INST),
        ),
        (
            "cancel-only, armed",
            |reg| {
                reg.kill(INST);
                reg.lift_kill(INST);
            },
            StateRefusal::CancelOnly(INST),
        ),
        (
            "cancel-only, disarmed",
            |reg| {
                reg.disarm(INST);
            },
            StateRefusal::CancelOnly(INST),
        ),
    ];
    for (case, into, refusal) in cases {
        let (mut reg, bid, ask) = quoting();
        into(&mut reg);
        let before = reg.len();
        for (name, order) in places() {
            assert_eq!(
                reg.place(order),
                Err(OmsError::State(refusal)),
                "{case}: {name}"
            );
        }
        // Refused before either cap: an order far over both is refused by the state too.
        assert_eq!(
            reg.place(placement(cid(), 100, 1_000_000)),
            Err(OmsError::State(refusal)),
            "{case}"
        );
        let plan = reg
            .place_batch(places().into_iter().map(|(_, o)| o).collect())
            .unwrap();
        assert_eq!(plan.command, None, "{case}");
        assert_eq!(plan.refused.len(), places().len(), "{case}");
        for (c, why) in &plan.refused {
            assert_eq!(why, &OmsError::State(refusal), "{case}: {c:?}");
        }
        for amend in amends() {
            assert_eq!(
                try_amend(&mut reg, (bid, ask), amend),
                Err(AmendRefusal::State(refusal)),
                "{case}: {}",
                amend.0
            );
        }
        // Nothing was registered, and nothing was reserved by a refused amend: both orders are
        // still live for an amend once the state admits one.
        assert_eq!(reg.len(), before, "{case}");
        assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5)), "{case}");
        assert_eq!(reg.resting_on(INST, Side::Sell), Some(lots(5)), "{case}");

        // Cancels and cancel-many are built in the same state.
        let caps = venue(false);
        let plan = reg.cancel_many(&[bid, ask], &caps);
        assert_eq!(plan.commands.len(), 1, "{case}");
        match plan.commands[0].command() {
            VenueCommand::CancelMany(items) => assert_eq!(items.len(), 2, "{case}"),
            other => panic!("{case}: expected a cancel-many, got {other:?}"),
        }
        let single = reg.cancellable(bid).unwrap().cancel(&caps);
        assert!(
            matches!(&single, CancelChoice::Send(cmd) if matches!(cmd.command(), VenueCommand::Cancel(_))),
            "{case}: {single:?}"
        );
    }
}

#[test]
fn lifting_the_kill_switch_while_armed_leaves_the_market_cancel_only_and_builds_no_order() {
    let (mut reg, bid, ask) = quoting();
    assert_eq!(seen(reg.entry(INST)), entry(true, EntryState::Quoting, 1));
    assert_eq!(seen(reg.kill(INST)), entry(true, EntryState::Killed, 2));
    assert_eq!(
        seen(reg.lift_kill(INST)),
        entry(true, EntryState::CancelOnly, 3)
    );
    for (name, order) in places() {
        assert_eq!(
            reg.place(order),
            Err(OmsError::State(StateRefusal::CancelOnly(INST))),
            "{name}"
        );
    }
    for amend in amends() {
        assert_eq!(
            try_amend(&mut reg, (bid, ask), amend),
            Err(AmendRefusal::State(StateRefusal::CancelOnly(INST))),
            "{}",
            amend.0
        );
    }
    // Lifting it again, or lifting it on a market never killed, changes nothing.
    assert_eq!(
        seen(reg.lift_kill(INST)),
        entry(true, EntryState::CancelOnly, 3)
    );
    assert_eq!(
        seen(reg.lift_kill(OTHER)),
        entry(false, EntryState::CancelOnly, 0)
    );
    // Start is still the owner's to press.
    assert_eq!(
        seen(reg.start(INST, Leases::none()).unwrap()),
        entry(true, EntryState::Quoting, 4)
    );
}

#[test]
fn a_fresh_registry_has_every_market_disarmed_and_cancel_only() {
    for reg in [Registry::new(), Registry::with_caps(caps()), seeded()] {
        for m in [INST, OTHER, InstrumentId::new(99)] {
            assert_eq!(seen(reg.entry(m)), entry(false, EntryState::CancelOnly, 0));
        }
    }
    // Caps configured and the position known are not enough: nothing is built until the
    // owner's Start, Flatten or Wind-down.
    let mut reg = seeded();
    assert_eq!(
        reg.place(placement(cid(), 100, 1)),
        Err(OmsError::State(StateRefusal::CancelOnly(INST)))
    );
    let plan = reg.place_batch(vec![placement(cid(), 100, 1)]).unwrap();
    assert_eq!(plan.command, None);
    assert!(reg.is_empty());
}

#[test]
fn flatten_or_wind_down_on_a_disarmed_market_arms_it_straight_into_exit() {
    type Call = fn(&mut Registry, InstrumentId, Leases) -> Result<MarketEntry, ArmRefusal>;
    let calls: [(Call, ExitKind); 2] = [
        (Registry::flatten, ExitKind::Flatten),
        (Registry::wind_down, ExitKind::WindDown),
    ];
    for (call, kind) in calls {
        let mut reg = seeded();
        let leases = leases(&reg, INST);
        // One call, one change: it never passes through Quoting.
        assert_eq!(
            seen(call(&mut reg, INST, leases).unwrap()),
            entry(true, EntryState::Exit(kind), 1)
        );
        // No ordinary order is built in Exit (its own admission is FBC-7gl's; until then Exit
        // builds nothing).
        assert_eq!(
            reg.place(placement(cid(), 100, 1)),
            Err(OmsError::State(StateRefusal::Exit(INST)))
        );
        let plan = reg.place_batch(vec![placement(cid(), 100, 1)]).unwrap();
        assert_eq!(plan.command, None);
        assert!(reg.is_empty());
        // The other exit, on the armed market, moves it to that Exit without a lease.
        let (other, other_kind) = calls
            .into_iter()
            .find(|(_, k)| *k != kind)
            .expect("two exits");
        assert_eq!(
            seen(other(&mut reg, INST, Leases::none()).unwrap()),
            entry(true, EntryState::Exit(other_kind), 2)
        );
        // The same exit again changes nothing.
        assert_eq!(
            seen(other(&mut reg, INST, Leases::none()).unwrap()),
            entry(true, EntryState::Exit(other_kind), 2)
        );
    }
}

#[test]
fn exit_refuses_an_amend_until_its_admission_is_built() {
    let (mut reg, bid, ask) = quoting();
    reg.flatten(INST, Leases::none()).unwrap();
    for amend in amends() {
        assert_eq!(
            try_amend(&mut reg, (bid, ask), amend),
            Err(AmendRefusal::State(StateRefusal::Exit(INST))),
            "{}",
            amend.0
        );
    }
}

#[test]
fn a_disarm_leaves_the_market_cancel_only_and_releases_its_leases() {
    // From Quoting and from Exit.
    for exit in [false, true] {
        let mut reg = seeded();
        start(&mut reg, INST);
        if exit {
            reg.wind_down(INST, Leases::none()).unwrap();
        }
        let generation = reg.entry(INST).generation().get();
        assert_eq!(
            seen(reg.disarm(INST)),
            entry(false, EntryState::CancelOnly, generation + 1)
        );
        assert_eq!(
            reg.place(placement(cid(), 100, 1)),
            Err(OmsError::State(StateRefusal::CancelOnly(INST)))
        );
        // A disarmed market's leases are free again, the account's with its last market's.
        let keys = reg.lease_keys().unwrap();
        drop(
            MarketLease::acquire(
                &lease_dir(),
                keys.venue(),
                keys.account(),
                &symbol(&wire(INST)),
            )
            .unwrap(),
        );
        assert!(account_lease(&reg).is_some());
        // Disarming a disarmed market changes nothing; arming again takes Start, Flatten or
        // Wind-down with the leases.
        assert_eq!(
            seen(reg.disarm(INST)),
            entry(false, EntryState::CancelOnly, generation + 1)
        );
        assert_eq!(
            reg.start(INST, Leases::none()),
            Err(ArmRefusal::NoMarketLease(INST))
        );
    }
    // A Killed market disarmed stays Killed.
    let mut reg = seeded();
    start(&mut reg, INST);
    reg.kill(INST);
    assert_eq!(seen(reg.disarm(INST)), entry(false, EntryState::Killed, 3));
}

#[test]
fn the_account_lease_is_held_while_any_market_is_armed() {
    let mut reg = seeded();
    start(&mut reg, INST);
    // The registry holds the account lease: OTHER arms with its market lease alone.
    assert!(account_lease(&reg).is_none());
    let lease = Leases::market(market_lease(&reg, OTHER));
    assert_eq!(
        seen(reg.start(OTHER, lease).unwrap()),
        entry(true, EntryState::Quoting, 1)
    );
    reg.disarm(INST);
    assert!(account_lease(&reg).is_none(), "OTHER is still armed");
    reg.disarm(OTHER);
    assert!(account_lease(&reg).is_some(), "no market is armed");
}

#[test]
fn only_start_reaches_quoting() {
    let mut reg = seeded();
    // Every other call, in every order the test walks, leaves the market short of Quoting.
    let leases = leases(&reg, INST);
    reg.flatten(INST, leases).unwrap();
    reg.wind_down(INST, Leases::none()).unwrap();
    reg.kill(INST);
    reg.lift_kill(INST);
    assert_eq!(reg.entry(INST).state(), EntryState::CancelOnly);
    reg.flatten(INST, Leases::none()).unwrap();
    reg.disarm(INST);
    reg.lift_kill(INST);
    assert_eq!(reg.entry(INST).state(), EntryState::CancelOnly);
    // Start reaches it from Cancel-only disarmed, and from Exit armed.
    assert_eq!(start(&mut reg, INST).state(), EntryState::Quoting);
    reg.flatten(INST, Leases::none()).unwrap();
    assert_eq!(
        reg.start(INST, Leases::none()).unwrap().state(),
        EntryState::Quoting
    );
    // Start on a Quoting market changes nothing.
    let generation = reg.entry(INST).generation();
    assert_eq!(
        reg.start(INST, Leases::none()).unwrap().generation(),
        generation
    );
    // Start, Flatten and Wind-down are each refused for a Killed market, armed or not.
    for disarmed in [false, true] {
        reg.kill(INST);
        if disarmed {
            reg.disarm(INST);
        }
        let before = seen(reg.entry(INST));
        assert_eq!(
            reg.start(INST, Leases::none()),
            Err(ArmRefusal::Killed(INST))
        );
        assert_eq!(
            reg.flatten(INST, Leases::none()),
            Err(ArmRefusal::Killed(INST))
        );
        assert_eq!(
            reg.wind_down(INST, Leases::none()),
            Err(ArmRefusal::Killed(INST))
        );
        assert_eq!(seen(reg.entry(INST)), before);
    }
}

type Arm = fn(&mut Registry, InstrumentId, Leases) -> Result<MarketEntry, ArmRefusal>;

/// Start, Flatten and Wind-down.
fn arming_calls() -> [(&'static str, Arm); 3] {
    [
        ("start", Registry::start),
        ("flatten", Registry::flatten),
        ("wind-down", Registry::wind_down),
    ]
}

/// Calls `call` on a fresh market of `reg` with `leases` and asserts it refused with `why`,
/// the market disarmed, Cancel-only and at generation 0 as before.
fn refused(reg: &mut Registry, name: &str, call: Arm, leases: Leases, why: ArmRefusal) {
    let before = seen(reg.entry(INST));
    assert_eq!(call(reg, INST, leases), Err(why), "{name}");
    assert_eq!(seen(reg.entry(INST)), before, "{name}");
    assert_eq!(before, entry(false, EntryState::CancelOnly, 0), "{name}");
}

#[test]
fn arming_is_refused_without_the_market_lease_changing_nothing() {
    for (name, call) in arming_calls() {
        let mut reg = seeded();
        let account = account_lease(&reg).unwrap();
        refused(
            &mut reg,
            name,
            call,
            Leases::none().with_account(account),
            ArmRefusal::NoMarketLease(INST),
        );
        // Another market's lease, or this symbol's under another account or venue, is not it.
        let other = Leases::market(market_lease(&reg, OTHER));
        refused(
            &mut reg,
            name,
            call,
            other,
            ArmRefusal::WrongMarketLease(INST),
        );
        let symbol = symbol(&wire(INST));
        let elsewhere = MarketLease::acquire(&lease_dir(), "elsewhere", "acct-x", &symbol).unwrap();
        refused(
            &mut reg,
            name,
            call,
            Leases::market(elsewhere),
            ArmRefusal::WrongMarketLease(INST),
        );
        let keys = reg.lease_keys().unwrap().clone();
        let other_account = MarketLease::acquire(
            &lease_dir(),
            keys.venue(),
            &format!("{}-other", keys.account()),
            &symbol,
        )
        .unwrap();
        refused(
            &mut reg,
            name,
            call,
            Leases::market(other_account),
            ArmRefusal::WrongMarketLease(INST),
        );
        // A refused call took nothing: the leases are free and the call succeeds with them.
        let leases = leases(&reg, INST);
        assert!(call(&mut reg, INST, leases).unwrap().armed(), "{name}");
    }
}

#[test]
fn arming_is_refused_without_the_account_lease_where_the_nonce_scope_is_per_account() {
    for (name, call) in arming_calls() {
        let mut reg = seeded();
        let lease = Leases::market(market_lease(&reg, INST));
        refused(
            &mut reg,
            name,
            call,
            lease,
            ArmRefusal::NoAccountLease(INST),
        );
        // Another account's lease is not this one's.
        let keys = reg.lease_keys().unwrap().clone();
        let wrong = fbc_core::AccountLease::acquire(
            &lease_dir(),
            keys.venue(),
            &format!("{}-other", keys.account()),
        )
        .unwrap();
        let lease = Leases::market(market_lease(&reg, INST)).with_account(wrong);
        refused(
            &mut reg,
            name,
            call,
            lease,
            ArmRefusal::WrongAccountLease(INST),
        );
        // With it, armed.
        let leases = leases(&reg, INST);
        assert!(call(&mut reg, INST, leases).unwrap().armed(), "{name}");
    }
    // Where the nonces are not per account, the market lease alone arms it.
    for scope in [NonceScope::PerSigner, NonceScope::Random, NonceScope::None] {
        for (name, call) in arming_calls() {
            let mut reg = Registry::with_caps(caps()).with_lease_keys(lease_keys(scope));
            reg.seed_position(INST, SignedLots(0)).unwrap();
            let lease = Leases::market(market_lease(&reg, INST));
            assert!(
                call(&mut reg, INST, lease).unwrap().armed(),
                "{scope:?} {name}"
            );
        }
    }
}

#[test]
fn arming_is_refused_for_a_market_the_lease_names_do_not_cover() {
    for (name, call) in arming_calls() {
        // No names at all.
        let mut reg = Registry::with_caps(caps());
        reg.seed_position(INST, SignedLots(0)).unwrap();
        let throwaway = lease_keys(NonceScope::PerSigner);
        let lease = MarketLease::acquire(
            &lease_dir(),
            throwaway.venue(),
            throwaway.account(),
            &symbol(&wire(INST)),
        )
        .unwrap();
        refused(
            &mut reg,
            name,
            call,
            Leases::market(lease),
            ArmRefusal::NotNamed(INST),
        );
        assert!(reg.lease_keys().is_none());
        // Names that leave the market out.
        let keys = LeaseKeys::new("synthetic", "acct-unnamed", NonceScope::PerSigner);
        let mut reg = Registry::with_caps(caps()).with_lease_keys(keys);
        reg.seed_position(INST, SignedLots(0)).unwrap();
        refused(
            &mut reg,
            name,
            call,
            Leases::none(),
            ArmRefusal::NotNamed(INST),
        );
    }
}

fn snapshot(positions: &[(InstrumentId, i64)]) -> ResyncSnapshot {
    ResyncSnapshot {
        watermark: WallNs(1_000),
        requested_at: MonoNs(1_000),
        orders: vec![],
        positions: positions.iter().map(|&(m, p)| (m, SignedLots(p))).collect(),
    }
}

fn ladder_cfg() -> LadderConfig {
    LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap()
}

fn with_source(source: SnapshotSource) -> OrderCaps {
    OrderCaps {
        snapshot_source: source,
        ..order_caps()
    }
}

#[test]
fn arming_is_refused_before_the_first_trustworthy_resync_changing_nothing() {
    for (name, call) in arming_calls() {
        let mut reg = unseeded();
        let leases_now = leases(&reg, INST);
        refused(
            &mut reg,
            name,
            call,
            leases_now,
            ArmRefusal::PositionUnknown(INST),
        );
        // A resync from a source that can be stale seeds nothing: still refused.
        let report = reg
            .resync(
                &ladder_cfg(),
                &with_source(SnapshotSource::Untrustworthy),
                &snapshot(&[(INST, 3)]),
                OrderKey {
                    venue: None,
                    ingest: 1,
                },
            )
            .unwrap();
        assert!(report.untrustworthy);
        let leases_now = leases(&reg, INST);
        refused(
            &mut reg,
            name,
            call,
            leases_now,
            ArmRefusal::PositionUnknown(INST),
        );
        // The first trustworthy resync seeds it, and the call arms the market.
        reg.resync(
            &ladder_cfg(),
            &with_source(SnapshotSource::Trustworthy),
            &snapshot(&[(INST, 3)]),
            OrderKey {
                venue: None,
                ingest: 2,
            },
        )
        .unwrap();
        assert_eq!(reg.position(INST), Some(SignedLots(3)));
        let leases_now = leases(&reg, INST);
        assert!(call(&mut reg, INST, leases_now).unwrap().armed(), "{name}");
    }
}

#[test]
fn no_resync_or_ladder_timer_moves_a_market_toward_exit_or_quoting() {
    let mut reg = unseeded();
    let caps = with_source(SnapshotSource::Trustworthy);
    // A seeding resync, a later one and the ladder's timer leave both markets as they were.
    for ingest in 1..=2 {
        reg.resync(
            &ladder_cfg(),
            &caps,
            &snapshot(&[(INST, 3)]),
            OrderKey {
                venue: None,
                ingest,
            },
        )
        .unwrap();
        reg.ladder(&ladder_cfg(), &caps, MonoNs(60_000_000_000));
        assert_eq!(
            seen(reg.entry(INST)),
            entry(false, EntryState::CancelOnly, 0)
        );
    }
    // Killed and lifted: Cancel-only, through every resync and tick after.
    start(&mut reg, INST);
    reg.kill(INST);
    reg.lift_kill(INST);
    let after = seen(reg.entry(INST));
    reg.resync(
        &ladder_cfg(),
        &caps,
        &snapshot(&[(INST, 3)]),
        OrderKey {
            venue: None,
            ingest: 3,
        },
    )
    .unwrap();
    reg.ladder(&ladder_cfg(), &caps, MonoNs(120_000_000_000));
    assert_eq!(seen(reg.entry(INST)), after);
    assert_eq!(after, entry(true, EntryState::CancelOnly, 3));
}

#[test]
fn each_market_keeps_its_own_state_and_generation() {
    let mut reg = seeded();
    start(&mut reg, INST);
    reg.kill(OTHER);
    assert_eq!(seen(reg.entry(INST)), entry(true, EntryState::Quoting, 1));
    assert_eq!(seen(reg.entry(OTHER)), entry(false, EntryState::Killed, 1));
    // INST still builds while OTHER is killed.
    assert!(reg.place(placement(cid(), 100, 1)).is_ok());
    // Killing a killed market changes nothing.
    assert_eq!(seen(reg.kill(OTHER)), entry(false, EntryState::Killed, 1));
}

#[test]
fn every_refusal_says_what_refused_it() {
    let refusals: Vec<Box<dyn std::error::Error>> = vec![
        Box::new(ArmRefusal::Killed(INST)),
        Box::new(ArmRefusal::PositionUnknown(INST)),
        Box::new(ArmRefusal::NotNamed(INST)),
        Box::new(ArmRefusal::NoMarketLease(INST)),
        Box::new(ArmRefusal::WrongMarketLease(INST)),
        Box::new(ArmRefusal::NoAccountLease(INST)),
        Box::new(ArmRefusal::WrongAccountLease(INST)),
        Box::new(StateRefusal::Killed(INST)),
        Box::new(StateRefusal::CancelOnly(INST)),
        Box::new(StateRefusal::Exit(INST)),
        Box::new(OmsError::State(StateRefusal::Killed(INST))),
    ];
    let texts: Vec<String> = refusals.iter().map(|r| r.to_string()).collect();
    for text in &texts {
        assert!(text.contains("InstrumentId(1)"), "{text}");
    }
    let unique: std::collections::HashSet<&String> = texts.iter().collect();
    assert_eq!(unique.len(), texts.len(), "{texts:?}");
    let keys = LeaseKeys::new("v", "a", NonceScope::PerSigner).with_market(INST, symbol("S"));
    assert_eq!(
        (keys.venue(), keys.account(), keys.nonce_scope()),
        ("v", "a", NonceScope::PerSigner)
    );
    assert_eq!(keys.symbol(INST), Some(&symbol("S")));
    assert_eq!(keys.symbol(OTHER), None);
}
