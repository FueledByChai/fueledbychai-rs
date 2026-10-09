//! Cancels and fills in every market state (decision 0012's cancel and fill rules; 0005's I3,
//! I4 and I7).
//!
//! The kill switch's "cancel everything" for a market ([`Registry::cancel_everything`]) is an
//! instrument cancel-all only while the registry holds the market's exclusive lease, a
//! trustworthy resync has shown the account's open orders and no foreign-namespace,
//! non-canonical or unattributed order is in view on the market; otherwise it cancels our
//! orders on the market by explicit reference in a cancel-many, and no foreign-namespace
//! order is ever cancelled. Every resync names the Killed markets, whose cancel everything goes
//! out again, the orders whose cancel is in flight included. No account-scope cancel-all is
//! ever built, in any state. An own-namespace fill arriving in Killed or Cancel-only for an
//! order whose cancel is in flight moves the inventory once; a foreign-namespace or
//! non-canonical one is flagged and moves nothing.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the resting cap is set past any order here.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use arm::{lease_keys, start};
use common::{cid, fill, ident, lots, order_caps, placement, vid};
use fbc_core::{
    AckLevel, CancelBatch, CancelReason, CancelScope, CidMatch, ClientOrderId, FillEvent,
    InstrumentId, ItemRef, MonoNs, Namespace, NewOrder, NonceScope, OrderCaps, OrderKind, OrderRef,
    OrderUpdate, RefKind, RpcId, Side, SignedLots, SnapshotSource, SubmitOutcome, Support, TagSet,
    Ticks, VenueCommand, VenueOrderSnapshot, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, CancelAllRefusal, CancelEverything, CancelPlan, EntryState, FillLedger, FillRouted,
    Intent, LadderConfig, Leases, LedgerConfig, MarketCapsConfig, OrdState, OrderKey, OrderOp,
    PreTradeCaps, Registry, ResyncSnapshot, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// The position each scenario holds on `INST`: long 20.
const LONG: i64 = 20;
/// Another engine's namespace on the account.
const THEIRS: Namespace = Namespace::new(8);

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

/// A venue that cancels one instrument's orders, or the whole account's, in one request, and
/// batches cancels by venue id: the account cancel-all is declared so that never building one
/// is the OMS's choice, not the venue's.
fn venue() -> OrderCaps {
    OrderCaps {
        cancel_all_account: Support::Native,
        cancel_all_instrument: Support::Native,
        batch_cancel: Some(CancelBatch {
            max_items: 10,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..order_caps()
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

fn key(ingest: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest,
    }
}

fn snapshot(at: u64, orders: Vec<VenueOrderSnapshot>) -> ResyncSnapshot {
    ResyncSnapshot {
        watermark: WallNs(i64::try_from(at).unwrap()),
        requested_at: MonoNs(at),
        orders,
        positions: vec![(INST, SignedLots(LONG))],
    }
}

/// Applies a resync of `orders` from a snapshot source `source`.
fn resync_from(
    reg: &mut Registry,
    source: SnapshotSource,
    at: u64,
    orders: Vec<VenueOrderSnapshot>,
) -> fbc_oms::ResyncReport {
    let caps = OrderCaps {
        snapshot_source: source,
        ..venue()
    };
    reg.resync(&ladder_cfg(), &caps, &snapshot(at, orders), key(at))
        .unwrap()
}

/// A named registry under the caps whose first trustworthy resync seeded `INST` long `LONG`
/// and `OTHER` flat, showing no order.
fn viewed() -> Registry {
    let mut reg = arm::named(Registry::with_caps(caps()));
    resync_from(&mut reg, SnapshotSource::Trustworthy, 1_000, vec![]);
    assert_eq!(reg.position(INST), Some(SignedLots(LONG)));
    reg
}

fn sell(qty: i64, px: i64) -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(px) },
        ..placement(cid(), px, qty)
    }
}

fn on(market: InstrumentId, order: NewOrder) -> NewOrder {
    NewOrder {
        inst: market,
        ..order
    }
}

/// Places `order` (admitted) and acknowledges it under the venue id `v`: Open.
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

/// Both markets armed and Quoting, `INST` resting a buy of 5 at 100 (`v-bid`) and a sell of 5
/// at 101 (`v-ask`), `OTHER` a buy of 1 at 100 (`v-other`); then `INST`'s kill switch on.
fn killed(mut reg: Registry) -> (Registry, ClientOrderId, ClientOrderId) {
    start(&mut reg, INST);
    start(&mut reg, OTHER);
    let bid = open(&mut reg, placement(cid(), 100, 5), "v-bid");
    let ask = open(&mut reg, sell(5, 101), "v-ask");
    open(&mut reg, on(OTHER, placement(cid(), 100, 1)), "v-other");
    reg.kill(INST);
    assert_eq!(reg.entry(INST).state(), EntryState::Killed);
    (reg, bid, ask)
}

/// An order event on `INST` for an order of another engine (`cid`), in `state`.
fn theirs(cid: Option<CidMatch>, v: Option<&str>, state: VenueOrderState) -> OrderUpdate {
    OrderUpdate {
        cid,
        vid: v.map(vid),
        inst: INST,
        side: Side::Buy,
        state,
        cum_filled: lots(0),
        px: Some(Ticks(99)),
        qty: Some(lots(3)),
        post_only: None,
        reduce_only: None,
    }
}

/// Another engine's order as a resync's snapshot shows it on `market`.
fn shown_theirs(market: InstrumentId, cid: Option<CidMatch>, v: &str) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid,
        vid: vid(v),
        inst: market,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(99)),
        qty: lots(3),
        cum_filled: lots(0),
        post_only: None,
        reduce_only: None,
    }
}

/// Our order `c` as a resync's snapshot shows it, open under `v`.
fn shown_ours(c: ClientOrderId, v: &str, side: Side, px: i64) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(c)),
        vid: vid(v),
        inst: INST,
        side,
        state: VenueOrderState::Open,
        px: Some(Ticks(px)),
        qty: lots(5),
        cum_filled: lots(0),
        post_only: None,
        reduce_only: None,
    }
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 10_000,
        },
        WallNs(0),
    )
    .unwrap()
}

/// Delivers `f` through the ledger to the registry: what the registry did with it, or `None`
/// when the ledger refused it.
fn deliver(reg: &mut Registry, l: &mut FillLedger, f: &FillEvent) -> Option<FillRouted> {
    match l.admit(f, None, MonoNs(2_000)) {
        Admission::Apply(accepted) => Some(reg.apply_fill(accepted).unwrap()),
        _ => None,
    }
}

/// A fill of `qty` on `INST` for another engine's order (`who`), named `fid`.
fn foreign_fill(who: CidMatch, v: Option<&str>, qty: i64, fid: &str) -> FillEvent {
    FillEvent {
        cid: Some(who),
        ident: fbc_core::FillIdent::Venue {
            fill: common::fill_id(fid),
            vid: v.map(vid),
            cum_after: None,
        },
        ..fill(None, ident(fid), Side::Buy, qty, false)
    }
}

/// Our orders `cids` named by client id and venue id, as the cancel-many names them.
fn by_reference(reg: &Registry, cids: &[ClientOrderId]) -> Vec<OrderRef> {
    cids.iter()
        .map(|&c| {
            let rec = reg.get(c).unwrap();
            OrderRef::Both(c, rec.vid().unwrap().clone())
        })
        .collect()
}

/// The explicit cancels `out` built, refused for `why`: one cancel-many on `INST` naming our
/// `bid` and `ask` by reference and nothing else, no cancel-all, no item refused or waiting.
fn assert_explicit(
    reg: &Registry,
    case: &str,
    out: CancelEverything,
    why: CancelAllRefusal,
    ours: &[ClientOrderId],
) {
    let CancelEverything::Explicit { plan, why: given } = out else {
        panic!("{case}: expected explicit cancels, got {out:?}");
    };
    assert_eq!(given, why, "{case}");
    assert!(plan.refused.is_empty(), "{case}: {:?}", plan.refused);
    assert!(plan.awaiting_ack.is_empty(), "{case}");
    assert_eq!(plan.commands.len(), 1, "{case}: {plan:?}");
    let VenueCommand::CancelMany(items) = plan.commands[0].command() else {
        panic!("{case}: expected a cancel-many, got {plan:?}");
    };
    assert!(items.iter().all(|i| i.inst == INST), "{case}");
    let named: Vec<OrderRef> = items.iter().map(|i| i.target.clone()).collect();
    assert_eq!(named, by_reference(reg, ours), "{case}");
}

#[test]
fn while_killed_a_cancel_all_is_built_only_for_the_instrument_under_the_exclusive_lease_with_no_foreign_order_in_view()
 {
    let (mut reg, bid, ask) = killed(viewed());
    let out = reg.cancel_everything(INST, &venue());
    let CancelEverything::CancelAll {
        command,
        unanswered,
    } = out
    else {
        panic!("expected an instrument cancel-all, got {out:?}");
    };
    assert_eq!(
        command.command(),
        &VenueCommand::CancelAll(CancelScope::Instrument(INST))
    );
    // Every order of ours is acknowledged, so the cancel-all reaches them all.
    assert_eq!(unanswered, CancelPlan::default());
    // Nothing of the orders changed: they rest until the venue ends them, and count as
    // resting meanwhile.
    for c in [bid, ask] {
        assert_eq!(reg.get(c).unwrap().intent(), Intent::None);
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5)));
    assert_eq!(reg.resting_on(INST, Side::Sell), Some(lots(5)));
}

#[test]
fn a_cancel_all_comes_with_the_explicit_cancels_of_our_orders_the_venue_may_not_hold_yet() {
    let mut reg = viewed();
    start(&mut reg, INST);
    let bid = open(&mut reg, placement(cid(), 100, 5), "v-bid");
    // One placement unanswered (PendingNew), one whose answer never came (Unknown).
    let pending = placement(cid(), 99, 1);
    let pending_cid = pending.cid;
    arm::place_issued(&mut reg, pending);
    let lost = placement(cid(), 98, 1);
    let lost_cid = lost.cid;
    arm::place_issued(&mut reg, lost);
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: None,
    };
    reg.on_outcome(
        lost_cid,
        OrderOp::Place,
        &item,
        &SubmitOutcome::Unknown,
        MonoNs(1),
    )
    .unwrap();
    assert_eq!(reg.get(lost_cid).unwrap().state(), OrdState::Unknown);
    reg.kill(INST);

    // A venue that cancels by client id before the acknowledgement: both go out by explicit
    // reference beside the cancel-all; the acknowledged bid is left to the cancel-all.
    let by_client = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        cancel_before_ack: true,
        ..venue()
    };
    let out = reg.cancel_everything(INST, &by_client);
    let CancelEverything::CancelAll { unanswered, .. } = out else {
        panic!("expected an instrument cancel-all, got {out:?}");
    };
    let mut named: Vec<OrderRef> = unanswered
        .commands
        .iter()
        .map(|cmd| match cmd.command() {
            VenueCommand::Cancel(c) => c.target.clone(),
            other => panic!("expected a single cancel, got {other:?}"),
        })
        .collect();
    named.sort_by_key(|r| format!("{r:?}"));
    let mut want = vec![OrderRef::Client(pending_cid), OrderRef::Client(lost_cid)];
    want.sort_by_key(|r| format!("{r:?}"));
    assert_eq!(named, want);
    assert!(
        !named
            .iter()
            .any(|r| matches!(r, OrderRef::Both(c, _) if *c == bid))
    );

    // On a venue that cancels by venue id alone, they wait for their acknowledgement and are
    // due the moment it lands.
    let out = reg.cancel_everything(INST, &venue());
    let CancelEverything::CancelAll { unanswered, .. } = out else {
        panic!("expected an instrument cancel-all, got {out:?}");
    };
    assert!(unanswered.commands.is_empty());
    let mut waiting = unanswered.awaiting_ack;
    waiting.sort();
    let mut want = vec![pending_cid, lost_cid];
    want.sort();
    assert_eq!(waiting, want);
    let ack = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid("v-pending")),
    };
    reg.on_outcome(
        pending_cid,
        OrderOp::Place,
        &ack,
        &SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        },
        MonoNs(2),
    )
    .unwrap();
    assert_eq!(reg.cancels_due(&venue()), vec![pending_cid]);
}

#[test]
fn otherwise_our_orders_are_cancelled_by_explicit_reference_and_no_foreign_order_is() {
    type Into = fn(&mut Registry, &mut FillLedger);
    let cases: [(&str, Into, CancelAllRefusal); 8] = [
        (
            "the market disarmed: no exclusive lease held",
            |reg, _| {
                reg.disarm(INST);
            },
            CancelAllRefusal::NoExclusiveLease,
        ),
        (
            "lease names given again for another account",
            |reg, _| {
                let given = std::mem::take(reg);
                *reg = given.with_lease_keys(lease_keys(NonceScope::PerAccountMonotonic));
            },
            CancelAllRefusal::NoExclusiveLease,
        ),
        (
            "another engine's order in a resync's snapshot",
            |reg, _| {
                let theirs = shown_theirs(INST, Some(CidMatch::Foreign(THEIRS)), "v-theirs");
                resync_from(reg, SnapshotSource::Trustworthy, 3_000, vec![theirs]);
            },
            CancelAllRefusal::ForeignInView,
        ),
        (
            "another engine's order in an order event",
            |reg, _| {
                let u = theirs(
                    Some(CidMatch::Foreign(THEIRS)),
                    Some("v-theirs"),
                    VenueOrderState::Open,
                );
                reg.apply_update(&u, key(3_000));
            },
            CancelAllRefusal::ForeignInView,
        ),
        (
            "another system's order (non-canonical client id) in an order event",
            |reg, _| {
                let u = theirs(
                    Some(CidMatch::Unparseable),
                    Some("v-theirs"),
                    VenueOrderState::Open,
                );
                reg.apply_update(&u, key(3_000));
            },
            CancelAllRefusal::ForeignInView,
        ),
        (
            "an order event nothing attributes, with no venue id",
            |reg, _| {
                reg.apply_update(&theirs(None, None, VenueOrderState::Open), key(3_000));
            },
            CancelAllRefusal::ForeignInView,
        ),
        (
            "another engine's fill",
            |reg, l| {
                let f = foreign_fill(CidMatch::Foreign(THEIRS), Some("v-theirs"), 1, "f-theirs");
                assert_eq!(
                    deliver(reg, l, &f),
                    Some(FillRouted::Foreign(THEIRS)),
                    "flagged"
                );
            },
            CancelAllRefusal::ForeignInView,
        ),
        (
            "a fill nothing attributes",
            |reg, l| {
                let mut f = fill(None, ident("f-nobody"), Side::Buy, 1, false);
                f.ident = fbc_core::FillIdent::Venue {
                    fill: common::fill_id("f-nobody"),
                    vid: Some(vid("v-nobody")),
                    cum_after: None,
                };
                assert_eq!(deliver(reg, l, &f), Some(FillRouted::Unattributed));
            },
            CancelAllRefusal::ForeignInView,
        ),
    ];
    for (case, into, why) in cases {
        let (mut reg, bid, ask) = killed(viewed());
        let mut l = ledger();
        into(&mut reg, &mut l);
        assert_eq!(reg.entry(INST).state(), EntryState::Killed, "{case}");
        let out = reg.cancel_everything(INST, &venue());
        assert_explicit(&reg, case, out, why, &[bid, ask]);
        // Nothing foreign moved the inventory.
        assert_eq!(reg.position(INST), Some(SignedLots(LONG)), "{case}");
    }

    // A venue with no instrument cancel-all.
    let (mut reg, bid, ask) = killed(viewed());
    let no_cancel_all = OrderCaps {
        cancel_all_instrument: Support::Unsupported,
        ..venue()
    };
    let out = reg.cancel_everything(INST, &no_cancel_all);
    assert_explicit(
        &reg,
        "no instrument cancel-all",
        out,
        CancelAllRefusal::Unsupported,
        &[bid, ask],
    );

    // No trustworthy resync has shown the account's open orders: a hand seed (armable only in
    // a declared owner-assisted testnet run, decision 0067), and a resync from a snapshot
    // source that can be stale, show nothing of what rests.
    let mut hand =
        arm::named(Registry::with_caps(caps()).for_testnet_run(TestnetRun::owner_assisted()));
    hand.seed_position(INST, SignedLots(LONG)).unwrap();
    hand.seed_position(OTHER, SignedLots(0)).unwrap();
    resync_from(&mut hand, SnapshotSource::Untrustworthy, 1_500, vec![]);
    let (mut reg, bid, ask) = killed(hand);
    let out = reg.cancel_everything(INST, &venue());
    assert_explicit(
        &reg,
        "no trustworthy resync",
        out,
        CancelAllRefusal::NotViewed,
        &[bid, ask],
    );
}

#[test]
fn a_foreign_order_leaves_the_view_only_when_the_venue_ends_it() {
    let (mut reg, bid, ask) = killed(viewed());
    let caps = venue();
    let foreign = Some(CidMatch::Foreign(THEIRS));
    let in_view = |reg: &mut Registry, case: &str| {
        let out = reg.cancel_everything(INST, &caps);
        assert_explicit(reg, case, out, CancelAllRefusal::ForeignInView, &[bid, ask]);
    };
    let clear = |reg: &mut Registry, case: &str| {
        let out = reg.cancel_everything(INST, &caps);
        assert!(
            matches!(&out, CancelEverything::CancelAll { .. }),
            "{case}: {out:?}"
        );
    };

    // Our own namespace's order the registry does not hold (an orphan, FBC-840's) is not
    // another's: an order event or a snapshot showing it puts nothing in view.
    let orphan = theirs(
        Some(CidMatch::Ours(cid())),
        Some("v-orphan"),
        VenueOrderState::Open,
    );
    reg.apply_update(&orphan, key(5));
    let shown_orphan = shown_theirs(INST, Some(CidMatch::Ours(cid())), "v-orphan-2");
    resync_from(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![shown_orphan],
    );
    clear(&mut reg, "own orphans");

    // Another engine's order on the other market leaves this market's view clear.
    let elsewhere = OrderUpdate {
        inst: OTHER,
        ..theirs(foreign, Some("v-elsewhere"), VenueOrderState::Open)
    };
    reg.apply_update(&elsewhere, key(10));
    clear(&mut reg, "foreign order on another market");
    assert!(reg.foreign_in_view(OTHER));
    assert!(!reg.foreign_in_view(INST));

    // One rests here.
    reg.apply_update(
        &theirs(foreign, Some("v-1"), VenueOrderState::Open),
        key(11),
    );
    in_view(&mut reg, "open");
    // A later trustworthy resync not showing it does not clear it: an order seen after the
    // venue read the account is not in its snapshot.
    resync_from(&mut reg, SnapshotSource::Trustworthy, 3_000, vec![]);
    in_view(&mut reg, "absent from a later snapshot");
    // Amended under a new venue id: the new id is in view, the old one gone.
    reg.apply_update(
        &theirs(
            foreign,
            Some("v-1"),
            VenueOrderState::Amended {
                new_vid: Some(vid("v-2")),
            },
        ),
        key(12),
    );
    in_view(&mut reg, "amended");
    // An end under the old id ends nothing in view.
    reg.apply_update(
        &theirs(
            foreign,
            Some("v-1"),
            VenueOrderState::Canceled(CancelReason::Requested),
        ),
        key(13),
    );
    in_view(&mut reg, "old id ended");
    // An end naming no venue id ends nothing either.
    reg.apply_update(&theirs(foreign, None, VenueOrderState::Filled), key(14));
    in_view(&mut reg, "an end naming no order");
    // An amend that keeps the id keeps it in view.
    reg.apply_update(
        &theirs(
            foreign,
            Some("v-2"),
            VenueOrderState::Amended { new_vid: None },
        ),
        key(15),
    );
    in_view(&mut reg, "amended in place");
    // The venue ends it: the view is clear, and the cancel-all is built again.
    reg.apply_update(
        &theirs(foreign, Some("v-2"), VenueOrderState::Filled),
        key(16),
    );
    clear(&mut reg, "ended");

    // A resync from a source that can be stale still shows what rests: seen, it is in view.
    let shown = shown_theirs(INST, Some(CidMatch::Unparseable), "v-3");
    resync_from(&mut reg, SnapshotSource::Untrustworthy, 4_000, vec![shown]);
    in_view(&mut reg, "in an untrustworthy snapshot");
    reg.apply_update(
        &theirs(
            Some(CidMatch::Unparseable),
            Some("v-3"),
            VenueOrderState::Expired,
        ),
        key(17),
    );
    clear(&mut reg, "expired");

    // A fill names its order: in view until the venue ends that order.
    let mut l = ledger();
    let f = foreign_fill(CidMatch::Foreign(THEIRS), Some("v-4"), 1, "f-4");
    assert_eq!(
        deliver(&mut reg, &mut l, &f),
        Some(FillRouted::Foreign(THEIRS))
    );
    in_view(&mut reg, "foreign fill");
    reg.apply_update(
        &theirs(foreign, Some("v-4"), VenueOrderState::Filled),
        key(18),
    );
    clear(&mut reg, "filled");

    // An order nothing attributes and that names no venue id cannot be ended by any event:
    // it stays in view, and the market's cancel everything stays explicit.
    reg.apply_update(&theirs(None, None, VenueOrderState::Open), key(19));
    in_view(&mut reg, "nameless");
    reg.apply_update(&theirs(None, None, VenueOrderState::Filled), key(20));
    in_view(&mut reg, "nameless, an end naming nothing");
    // A foreign fill naming no venue id is nameless too.
    let (mut reg, bid2, ask2) = killed(viewed());
    let f = foreign_fill(CidMatch::Foreign(THEIRS), None, 1, "f-5");
    assert_eq!(
        deliver(&mut reg, &mut l, &f),
        Some(FillRouted::Foreign(THEIRS))
    );
    let out = reg.cancel_everything(INST, &caps);
    assert_explicit(
        &reg,
        "nameless fill",
        out,
        CancelAllRefusal::ForeignInView,
        &[bid2, ask2],
    );
}

#[test]
fn the_cancel_everything_is_due_again_on_each_resync_while_killed() {
    // A venue with no instrument cancel-all, so the cancels are explicit.
    let caps = OrderCaps {
        cancel_all_instrument: Support::Unsupported,
        ..venue()
    };
    let (mut reg, bid, ask) = killed(viewed());
    let out = reg.cancel_everything(INST, &caps);
    assert_explicit(
        &reg,
        "first",
        out,
        CancelAllRefusal::Unsupported,
        &[bid, ask],
    );
    // Both cancels go out and wait for their answer.
    reg.cancel_sent(bid, RpcId(1), MonoNs(1_500)).unwrap();
    reg.cancel_sent(ask, RpcId(2), MonoNs(1_500)).unwrap();

    // A resync shows both still open: it names the Killed market, and its cancel everything
    // names both again, their cancels in flight or not; the market not Killed is not named.
    let report = resync_from(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![
            shown_ours(bid, "v-bid", Side::Buy, 100),
            shown_ours(ask, "v-ask", Side::Sell, 101),
        ],
    );
    assert_eq!(report.cancel_everything, vec![INST]);
    for c in [bid, ask] {
        assert!(matches!(
            reg.get(c).unwrap().intent(),
            Intent::PendingCancel { .. }
        ));
    }
    let out = reg.cancel_everything(INST, &caps);
    assert_explicit(
        &reg,
        "after a resync",
        out,
        CancelAllRefusal::Unsupported,
        &[bid, ask],
    );

    // The venue ends the ask: the next resync's cancel everything names the bid alone.
    let ended = OrderUpdate {
        cid: Some(CidMatch::Ours(ask)),
        vid: Some(vid("v-ask")),
        inst: INST,
        side: Side::Sell,
        state: VenueOrderState::Canceled(CancelReason::Requested),
        cum_filled: lots(0),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(&ended, key(4_000));
    let report = resync_from(
        &mut reg,
        SnapshotSource::Trustworthy,
        5_000,
        vec![shown_ours(bid, "v-bid", Side::Buy, 100)],
    );
    assert_eq!(report.cancel_everything, vec![INST]);
    let out = reg.cancel_everything(INST, &caps);
    assert_explicit(
        &reg,
        "the ask ended",
        out,
        CancelAllRefusal::Unsupported,
        &[bid],
    );

    // Two Killed markets are named in id order; a lifted kill switch is named no more.
    reg.kill(OTHER);
    let report = resync_from(&mut reg, SnapshotSource::Trustworthy, 6_000, vec![]);
    assert_eq!(report.cancel_everything, vec![INST, OTHER]);
    reg.lift_kill(INST);
    reg.lift_kill(OTHER);
    let report = resync_from(&mut reg, SnapshotSource::Trustworthy, 7_000, vec![]);
    assert!(report.cancel_everything.is_empty());
    // A resync from a source that can be stale names them too.
    reg.kill(INST);
    let report = resync_from(&mut reg, SnapshotSource::Untrustworthy, 8_000, vec![]);
    assert_eq!(report.cancel_everything, vec![INST]);
}

#[test]
fn no_account_scope_cancel_all_is_ever_built_in_any_state() {
    type Into = fn(&mut Registry);
    let states: [(&str, Into, bool); 7] = [
        (
            "killed, armed",
            |reg| {
                reg.kill(INST);
            },
            true,
        ),
        (
            "killed, disarmed",
            |reg| {
                reg.kill(INST);
                reg.disarm(INST);
            },
            false,
        ),
        (
            "cancel-only, armed",
            |reg| {
                reg.kill(INST);
                reg.lift_kill(INST);
            },
            true,
        ),
        (
            "cancel-only, disarmed",
            |reg| {
                reg.disarm(INST);
            },
            false,
        ),
        (
            "exit (flatten)",
            |reg| {
                reg.flatten(INST, Leases::none()).unwrap();
            },
            true,
        ),
        (
            "exit (wind-down)",
            |reg| {
                reg.wind_down(INST, Leases::none()).unwrap();
            },
            true,
        ),
        ("quoting", |_| {}, true),
    ];
    for (case, into, leased) in states {
        for foreign in [false, true] {
            let mut reg = viewed();
            start(&mut reg, INST);
            let bid = open(&mut reg, placement(cid(), 100, 5), "v-bid");
            let ask = open(&mut reg, sell(5, 101), "v-ask");
            into(&mut reg);
            if foreign {
                let u = theirs(
                    Some(CidMatch::Foreign(THEIRS)),
                    Some("v-theirs"),
                    VenueOrderState::Open,
                );
                reg.apply_update(&u, key(3_000));
            }
            let case = format!("{case}, foreign in view: {foreign}");
            let out = reg.cancel_everything(INST, &venue());
            match out {
                CancelEverything::CancelAll {
                    command,
                    unanswered,
                } => {
                    assert!(leased && !foreign, "{case}");
                    assert_eq!(unanswered, CancelPlan::default(), "{case}");
                    assert_eq!(
                        command.command(),
                        &VenueCommand::CancelAll(CancelScope::Instrument(INST)),
                        "{case}"
                    );
                }
                explicit @ CancelEverything::Explicit { .. } => {
                    let why = if !leased {
                        CancelAllRefusal::NoExclusiveLease
                    } else {
                        CancelAllRefusal::ForeignInView
                    };
                    assert!(!leased || foreign, "{case}");
                    assert_explicit(&reg, &case, explicit, why, &[bid, ask]);
                }
            }
        }
    }
}

#[test]
fn an_own_fill_in_killed_or_cancel_only_for_an_order_whose_cancel_is_in_flight_moves_inventory_once()
 {
    type Into = fn(&mut Registry);
    let cases: [(&str, Into, EntryState); 4] = [
        (
            "killed, armed",
            |reg| {
                reg.kill(INST);
            },
            EntryState::Killed,
        ),
        (
            "killed, disarmed",
            |reg| {
                reg.kill(INST);
                reg.disarm(INST);
            },
            EntryState::Killed,
        ),
        (
            "cancel-only, armed",
            |reg| {
                reg.kill(INST);
                reg.lift_kill(INST);
            },
            EntryState::CancelOnly,
        ),
        (
            "cancel-only, disarmed",
            |reg| {
                reg.disarm(INST);
            },
            EntryState::CancelOnly,
        ),
    ];
    for (case, into, state) in cases {
        let mut reg = viewed();
        start(&mut reg, INST);
        let bid = open(&mut reg, placement(cid(), 100, 5), "v-bid");
        // The bid's cancel is built and sent; then the market's state changes.
        let choice = reg.cancellable(bid).unwrap().cancel(&venue());
        assert!(matches!(choice, fbc_oms::CancelChoice::Send(_)), "{case}");
        reg.cancel_sent(bid, RpcId(1), MonoNs(1_500)).unwrap();
        into(&mut reg);
        let entry = reg.entry(INST);
        assert_eq!(entry.state(), state, "{case}");
        assert!(
            matches!(reg.get(bid).unwrap().intent(), Intent::PendingCancel { .. }),
            "{case}"
        );

        // A fill of 2 races the cancel: the inventory moves once, however often it arrives.
        let mut l = ledger();
        let f = fill(Some(bid), ident("f-bid"), Side::Buy, 2, false);
        assert!(
            matches!(deliver(&mut reg, &mut l, &f), Some(FillRouted::Ours(c, _)) if c == bid),
            "{case}"
        );
        assert_eq!(reg.position(INST), Some(SignedLots(LONG + 2)), "{case}");
        assert_eq!(deliver(&mut reg, &mut l, &f), None, "{case}: duplicate");
        let replay = FillEvent {
            replay: true,
            ..f.clone()
        };
        assert_eq!(deliver(&mut reg, &mut l, &replay), None, "{case}: replay");
        assert_eq!(reg.position(INST), Some(SignedLots(LONG + 2)), "{case}");
        assert_eq!(reg.get(bid).unwrap().cum_fills(), lots(2), "{case}");
        // Its remainder still rests, and the cancel is still in flight.
        assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(3)), "{case}");
        assert!(
            matches!(reg.get(bid).unwrap().intent(), Intent::PendingCancel { .. }),
            "{case}"
        );

        // Another engine's fill, and another system's, are flagged and move nothing.
        let theirs = foreign_fill(CidMatch::Foreign(THEIRS), Some("v-t"), 4, "f-theirs");
        assert_eq!(
            deliver(&mut reg, &mut l, &theirs),
            Some(FillRouted::Foreign(THEIRS)),
            "{case}"
        );
        let other = foreign_fill(CidMatch::Unparseable, Some("v-o"), 4, "f-other");
        assert_eq!(
            deliver(&mut reg, &mut l, &other),
            Some(FillRouted::NotCanonical),
            "{case}"
        );
        assert_eq!(reg.position(INST), Some(SignedLots(LONG + 2)), "{case}");
        assert_eq!(reg.get(bid).unwrap().cum_fills(), lots(2), "{case}");

        // No fill changed the market's state.
        assert_eq!(reg.entry(INST), entry, "{case}");
    }
}

#[test]
fn every_refusal_says_why_no_cancel_all_was_built() {
    for (why, text) in [
        (CancelAllRefusal::Unsupported, "instrument cancel-all"),
        (CancelAllRefusal::NoExclusiveLease, "lease"),
        (CancelAllRefusal::NotViewed, "trustworthy resync"),
        (CancelAllRefusal::ForeignInView, "another"),
    ] {
        assert!(why.to_string().contains(text), "{why:?}: {why}");
    }
}
