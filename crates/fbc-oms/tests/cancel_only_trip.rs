//! FBC-8pm (decision 0139): a registry's `CancelOnlyTrip`, which the shard's journal trips when
//! it reaches its soft limit, moves every market of the registry in Quoting to Cancel-only, its
//! armed flag unchanged, and leaves Exit, Killed and Cancel-only as they were. The market reads
//! Cancel-only at once and builds no place, amend or batch item, while cancels and cancel-many
//! are still built; its state generation advances at the registry's next mutating call. A
//! place, batch or amend authorized in Quoting before the trip is refused at submit, before and
//! after the registry applies it, and one built before it and authorized after it is refused as
//! a change of state, while a cancel or cancel-many built before it goes through. A market in
//! Exit, the owner's Flatten or Wind-down, keeps building its reducing orders, and those
//! authorized before a trip go through (decision 0006). Only the owner's Start, Flatten or
//! Wind-down leaves Cancel-only: a lift of the kill switch, a resync or a disarm does not, a
//! Start made after the trip stands, and a later trip moves the market again, while a Flatten
//! or Wind-down made after it stands through every later trip.

#[path = "common/arm.rs"]
mod arm;
mod common;

use arm::{TEST_ACCT, leases, named, seed, start};
use common::{cid, lots, order_caps, placement, vid};
use fbc_core::{
    AckLevel, AmendAck, AmendCaps, AmendQty, CancelBatch, ClientOrderId, InstrumentId, ItemRef,
    MonoNs, NewOrder, OrderCaps, OrderKind, RefKind, Side, SubmitOutcome, TagSet, Ticks,
    VenueCommand,
};
use fbc_oms::{
    AmendRefusal, CancelChoice, EntryState, ExitKind, Leases, MarketCapsConfig, MarketEntry,
    OmsError, OrderOp, PreTradeCaps, Registry, StaleAuthorization, StateRefusal,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
const KILLED: InstrumentId = InstrumentId::new(3);
const IDLE: InstrumentId = InstrumentId::new(4);
/// The position on `INST`: long 20, so a sell reduces it.
const LONG: i64 = 20;

fn caps() -> PreTradeCaps {
    [INST, OTHER, KILLED, IDLE]
        .into_iter()
        .try_fold(PreTradeCaps::new(), |caps, m| {
            caps.with_market(
                m,
                MarketCapsConfig {
                    inventory: Some(lots(1_000)),
                    resting: Some(lots(1_000_000)),
                },
            )
        })
        .unwrap()
}

/// A named registry under the caps, `INST` long `LONG` and the others flat, seeded by a
/// trustworthy resync.
fn seeded() -> Registry {
    let mut reg = named(Registry::with_caps(caps()));
    seed(
        &mut reg,
        &[(INST, LONG), (OTHER, 0), (KILLED, 0), (IDLE, 0)],
    );
    reg
}

fn seen(e: MarketEntry) -> (bool, EntryState, u64) {
    (e.armed(), e.state(), e.generation().get())
}

/// Single and batch cancels by venue id, amends keeping it.
fn venue() -> OrderCaps {
    OrderCaps {
        amend: Some(AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id: true,
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

fn sell(qty: i64, px: i64) -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(px) },
        ..placement(cid(), px, qty)
    }
}

/// Places `order`, authorizes it and acknowledges it under the venue id `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    arm::place_issued(reg, order);
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid(v)),
    };
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    reg.on_outcome(c, OrderOp::Place, &item, &accepted, MonoNs(1))
        .unwrap();
    c
}

/// `INST` armed and Quoting, resting a buy of 5 at 100 and a sell of 5 at 101.
fn quoting() -> (Registry, ClientOrderId, ClientOrderId) {
    let mut reg = seeded();
    start(&mut reg, INST);
    let bid = open(&mut reg, placement(cid(), 100, 5), "v-bid");
    let ask = open(&mut reg, sell(5, 101), "v-ask");
    (reg, bid, ask)
}

#[test]
fn a_trip_moves_every_market_in_quoting_to_cancel_only_its_armed_flag_unchanged_and_leaves_exit_killed_and_cancel_only_as_they_were()
 {
    let mut reg = seeded();
    // INST Quoting, OTHER in Exit by the owner's Flatten, KILLED armed with its kill switch on,
    // IDLE never armed: disarmed and Cancel-only.
    start(&mut reg, INST);
    let flatten = leases(&reg, OTHER);
    reg.flatten(OTHER, flatten).unwrap();
    start(&mut reg, KILLED);
    reg.kill(KILLED);
    let before = [
        (INST, (true, EntryState::Quoting, 1)),
        (OTHER, (true, EntryState::Exit(ExitKind::Flatten), 1)),
        (KILLED, (true, EntryState::Killed, 2)),
        (IDLE, (false, EntryState::CancelOnly, 0)),
    ];
    for (m, want) in before {
        assert_eq!(seen(reg.entry(m)), want, "{m:?}");
    }
    // Another registry's markets are its own trip's.
    let mut other = seeded();
    start(&mut other, INST);

    let trip = reg.cancel_only_trip();
    assert_eq!(trip.trips(), 0);
    trip.trip();
    assert_eq!(trip.trips(), 1);
    // Every call hands out the same trip.
    assert_eq!(reg.cancel_only_trip().trips(), 1);
    assert!(format!("{trip:?}").contains("CancelOnlyTrip"));

    // Read at once: Quoting is Cancel-only, armed as it was, and Exit is still Exit; the
    // generations advance at the registry's next mutating call.
    let read = [
        (INST, (true, EntryState::CancelOnly, 1)),
        (OTHER, (true, EntryState::Exit(ExitKind::Flatten), 1)),
        (KILLED, (true, EntryState::Killed, 2)),
        (IDLE, (false, EntryState::CancelOnly, 0)),
    ];
    for (m, want) in read {
        assert_eq!(seen(reg.entry(m)), want, "{m:?}");
    }
    // A call that changes nothing of its own applies the trip first.
    assert_eq!(
        seen(reg.lift_kill(IDLE)),
        (false, EntryState::CancelOnly, 0)
    );
    let applied = [
        (INST, (true, EntryState::CancelOnly, 2)),
        (OTHER, (true, EntryState::Exit(ExitKind::Flatten), 1)),
        (KILLED, (true, EntryState::Killed, 2)),
        (IDLE, (false, EntryState::CancelOnly, 0)),
    ];
    for (m, want) in applied {
        assert_eq!(seen(reg.entry(m)), want, "{m:?}");
    }
    // Applied once: the next call moves nothing.
    reg.lift_kill(IDLE);
    for (m, want) in applied {
        assert_eq!(seen(reg.entry(m)), want, "{m:?}");
    }
    // The kill switch's lift still leads to Cancel-only, and no further.
    assert_eq!(
        seen(reg.lift_kill(KILLED)),
        (true, EntryState::CancelOnly, 3)
    );
    assert_eq!(seen(other.entry(INST)), (true, EntryState::Quoting, 1));
}

#[test]
fn after_a_trip_no_place_amend_or_batch_item_is_built_and_cancels_still_are() {
    let (mut reg, bid, ask) = quoting();
    let before = reg.len();
    reg.cancel_only_trip().trip();
    let refusal = OmsError::State(StateRefusal::CancelOnly(INST));
    // An ordinary buy, a reduce-only sell and a sell I6 admits because it reduces the position.
    let reducing = NewOrder {
        reduce_only: true,
        reducing: true,
        ..sell(5, 102)
    };
    for order in [placement(cid(), 99, 5), reducing, sell(10, 102)] {
        assert_eq!(reg.place(order), Err(refusal.clone()));
    }
    let plan = reg
        .place_batch(vec![placement(cid(), 98, 1), sell(1, 103)])
        .unwrap();
    assert_eq!(plan.command, None);
    assert_eq!(plan.refused.len(), 2);
    assert!(plan.refused.iter().all(|(_, why)| *why == refusal));
    let amend = reg
        .live(bid)
        .unwrap()
        .amend(&venue(), Ticks(99), lots(5), false);
    assert_eq!(
        amend.map(|cmd| cmd.command().clone()),
        Err(AmendRefusal::State(StateRefusal::CancelOnly(INST)))
    );
    assert_eq!(reg.len(), before);

    // Cancels and cancel-many are built in Cancel-only as in every state (decision 0012).
    let plan = reg.cancel_many(&[bid, ask], &venue());
    assert_eq!(plan.commands.len(), 1);
    assert!(matches!(
        plan.commands[0].command(),
        VenueCommand::CancelMany(items) if items.len() == 2
    ));
    let single = reg.cancellable(ask).unwrap().cancel(&venue());
    assert!(
        matches!(&single, CancelChoice::Send(cmd) if matches!(cmd.command(), VenueCommand::Cancel(_))),
        "{single:?}"
    );
}

#[test]
fn a_place_batch_or_amend_issued_before_a_trip_is_refused_at_submit_and_a_cancel_issued_before_it_is_not()
 {
    let (mut reg, bid, ask) = quoting();
    let place = reg.place(placement(cid(), 99, 5)).unwrap();
    let place = reg.authorize(TEST_ACCT, place).unwrap();
    let batch = reg
        .place_batch(vec![placement(cid(), 98, 1), placement(cid(), 97, 1)])
        .unwrap();
    let batch = reg.authorize(TEST_ACCT, batch.command.unwrap()).unwrap();
    let amend = reg
        .live(bid)
        .unwrap()
        .amend(&venue(), Ticks(99), lots(5), false)
        .unwrap();
    let amend = reg.authorize(TEST_ACCT, amend).unwrap();
    let CancelChoice::Send(cancel) = reg.cancellable(ask).unwrap().cancel(&venue()) else {
        panic!("an acknowledged order's cancel is sent");
    };
    let cancel = reg.authorize(TEST_ACCT, cancel).unwrap();
    let mut many = reg.cancel_many(&[bid], &venue());
    let many = reg.authorize(TEST_ACCT, many.commands.remove(0)).unwrap();
    for auth in [&place, &batch, &amend, &cancel, &many] {
        assert_eq!(auth.check_at_submit(), Ok(()));
    }

    // Tripped: refused at submit before the registry applied the trip (no call since) ...
    let trip = reg.cancel_only_trip();
    trip.trip();
    let stale = Err(StaleAuthorization::CancelOnly(INST));
    for auth in [&place, &batch, &amend] {
        assert_eq!(auth.check_at_submit(), stale);
    }
    // ... and after, the trip's reason first though the market's generation moved too.
    reg.lift_kill(INST);
    assert_eq!(seen(reg.entry(INST)), (true, EntryState::CancelOnly, 2));
    for auth in [&place, &batch, &amend] {
        assert_eq!(auth.check_at_submit(), stale);
    }
    assert!(
        StaleAuthorization::CancelOnly(INST)
            .to_string()
            .contains("Cancel-only")
    );
    // Cancels go through in every state.
    assert_eq!(cancel.check_at_submit(), Ok(()));
    assert_eq!(many.check_at_submit(), Ok(()));

    // Built before a trip and authorized after it: the authorization applied the trip, which
    // moved the market's generation, so it is refused as a change of state.
    let (mut fresh, _, _) = quoting();
    let built = fresh.place(placement(cid(), 99, 5)).unwrap();
    fresh.cancel_only_trip().trip();
    let late = fresh.authorize(TEST_ACCT, built).unwrap();
    assert!(
        matches!(
            late.check_at_submit(),
            Err(StaleAuthorization::StateChanged { market: INST, .. })
        ),
        "{late:?}"
    );

    // Once the owner's Start reached Quoting again, a place built then goes through.
    reg.start(INST, Leases::none()).unwrap();
    let again = reg.place(placement(cid(), 96, 1)).unwrap();
    let again = reg.authorize(TEST_ACCT, again).unwrap();
    assert_eq!(again.check_at_submit(), Ok(()));
    // Until the next trip.
    trip.trip();
    assert_eq!(again.check_at_submit(), stale);
}

#[test]
fn only_the_owners_start_flatten_or_wind_down_leaves_cancel_only_after_a_trip() {
    // A lift of the kill switch, a resync and a disarm leave it Cancel-only.
    let (mut reg, _, _) = quoting();
    reg.cancel_only_trip().trip();
    assert_eq!(seen(reg.lift_kill(INST)), (true, EntryState::CancelOnly, 2));
    seed(&mut reg, &[(INST, LONG), (OTHER, 0)]);
    assert_eq!(seen(reg.entry(INST)), (true, EntryState::CancelOnly, 2));
    assert_eq!(seen(reg.disarm(INST)), (false, EntryState::CancelOnly, 3));
    // Disarmed, Start arms it again with its leases, into Quoting.
    let given = leases(&reg, INST);
    assert_eq!(
        seen(reg.start(INST, given).unwrap()),
        (true, EntryState::Quoting, 4)
    );

    // Armed, each of the owner's three calls leaves it with no lease.
    type Call = fn(&mut Registry) -> MarketEntry;
    let calls: [(Call, EntryState); 3] = [
        (
            |reg| reg.start(INST, Leases::none()).unwrap(),
            EntryState::Quoting,
        ),
        (
            |reg| reg.flatten(INST, Leases::none()).unwrap(),
            EntryState::Exit(ExitKind::Flatten),
        ),
        (
            |reg| reg.wind_down(INST, Leases::none()).unwrap(),
            EntryState::Exit(ExitKind::WindDown),
        ),
    ];
    for (call, to) in calls {
        let (mut reg, _, _) = quoting();
        let trip = reg.cancel_only_trip();
        trip.trip();
        // Made right after the trip, before anything applied it: the trip comes first, so the
        // owner's call stands.
        assert_eq!(seen(call(&mut reg)), (true, to, 3), "{to:?}");
        reg.lift_kill(INST);
        assert_eq!(seen(reg.entry(INST)), (true, to, 3), "{to:?}");
        // A second trip moves Quoting again, and leaves the owner's exit as it is.
        trip.trip();
        let moved = if to == EntryState::Quoting {
            (EntryState::CancelOnly, 4)
        } else {
            (to, 3)
        };
        assert_eq!(
            seen(reg.entry(INST)),
            (true, moved.0, 3),
            "{to:?} read at once"
        );
        reg.lift_kill(INST);
        assert_eq!(seen(reg.entry(INST)), (true, moved.0, moved.1), "{to:?}");
    }

    // An order built after the owner's Start goes out; one built before the trip did not.
    let (mut reg, _, _) = quoting();
    reg.cancel_only_trip().trip();
    assert!(reg.place(placement(cid(), 99, 1)).is_err());
    reg.start(INST, Leases::none()).unwrap();
    assert!(reg.place(placement(cid(), 99, 1)).is_ok());
}

#[test]
fn an_exit_market_keeps_building_and_sending_its_reducing_orders_through_a_trip() {
    // INST long 20, in the owner's Flatten: only sells that reduce it are built.
    let mut reg = seeded();
    let given = leases(&reg, INST);
    let exit = (true, EntryState::Exit(ExitKind::Flatten), 1);
    assert_eq!(seen(reg.flatten(INST, given).unwrap()), exit);
    let reducing = |px| NewOrder {
        reduce_only: true,
        ..sell(5, px)
    };
    // Authorized before the trip, and built before it and authorized after it.
    let built = reg.place(reducing(102)).unwrap();
    let before = reg.authorize(TEST_ACCT, built).unwrap();
    let late = reg.place(reducing(103)).unwrap();

    let trip = reg.cancel_only_trip();
    trip.trip();
    // Exit stays Exit, read at once and applied: its generation does not move.
    assert_eq!(seen(reg.entry(INST)), exit);
    assert_eq!(before.check_at_submit(), Ok(()));
    let late = reg.authorize(TEST_ACCT, late).unwrap();
    assert_eq!(seen(reg.entry(INST)), exit);
    // Built and authorized after the trip, and every one through a further trip.
    let after = reg.place(reducing(104)).unwrap();
    let after = reg.authorize(TEST_ACCT, after).unwrap();
    trip.trip();
    for auth in [&before, &late, &after] {
        assert_eq!(auth.check_at_submit(), Ok(()), "{auth:?}");
    }
    reg.lift_kill(INST);
    assert_eq!(seen(reg.entry(INST)), exit);
    for auth in [&before, &late, &after] {
        assert_eq!(auth.check_at_submit(), Ok(()), "{auth:?}");
    }
    // Exit's own admission stands: an order that adds to the position is refused as Exit
    // refuses it, not as Cancel-only.
    assert!(
        matches!(
            reg.place(placement(cid(), 99, 5)),
            Err(OmsError::State(StateRefusal::Exit(_)))
        ),
        "a buy adds to a long position"
    );
    // A change of the market's state still refuses them at submit, as before (decision 0060).
    reg.kill(INST);
    for auth in [&before, &late, &after] {
        assert!(
            matches!(
                auth.check_at_submit(),
                Err(StaleAuthorization::StateChanged { market: INST, .. })
            ),
            "{auth:?}"
        );
    }
}
