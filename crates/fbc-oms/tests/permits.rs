//! Permits (decision 0005): a cancel's reference for every combination of a known venue id,
//! the acknowledgement, the venue's cancel references and `cancel_before_ack`, the cancel
//! waiting for the acknowledgement and due the moment it lands; a cancel-many carrying only
//! the items whose reference its batch declares, the others sent as single cancels; an amend
//! built only through a Live permit and refused where the venue's caps cannot amend the order;
//! and a foreign-namespace or non-canonical order never cancelled one by one, its fills
//! flagged and moving nothing (I4). The compile-fail half is `tests/compile_fail.rs`.

mod common;

use std::time::Duration;

use common::{cid, fill, ident, lots, order_caps, placement, update, vid};
use fbc_core::{
    AckLevel, AmendAck, AmendCaps, AmendOrder, AmendQty, CancelBatch, CancelOrder, CidMatch,
    ClientOrderId, InstrumentId, ItemRef, MonoNs, Namespace, OrderCaps, OrderKind, OrderRef,
    RefKind, RpcId, Side, SignedLots, SubmitOutcome, TagSet, Ticks, VenueCommand, VenueOrderId,
    VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, AmendRefusal, CancelChoice, FillLedger, FillRouted, LedgerConfig, MarketCaps,
    OmsError, OrdState, OrderKey, OrderOp, PermitRefusal, PermittedCommand, PreTradeCaps, Registry,
};

const INST: InstrumentId = InstrumentId::new(1);

/// A registry whose inventory cap (0005's I6) is far above any order here: the permits are
/// what these tests judge; the caps are `tests/caps.rs`'s.
fn registry() -> Registry {
    let mut reg = Registry::with_caps(PreTradeCaps::new().with_market(
        INST,
        MarketCaps {
            inventory: lots(1_000_000),
        },
    ));
    reg.seed_position(INST, SignedLots(0)).unwrap();
    reg
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// The placement's outcome for `cid`, the venue naming it `vid` (or not).
fn ack(reg: &mut Registry, cid: ClientOrderId, vid: Option<VenueOrderId>) {
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid,
    };
    reg.on_outcome(cid, OrderOp::Place, &item, &accepted(), MonoNs(1))
        .unwrap();
}

/// How far the venue has acknowledged an order.
#[derive(Copy, Clone, Debug, PartialEq)]
enum Ack {
    /// Sent, no answer yet (PendingNew).
    Pending,
    /// Sent, unanswered by its deadline (Unknown).
    Unknown,
    /// Accepted without the venue's id.
    AckedNoVid,
    /// Accepted under the venue's id.
    AckedVid,
}

/// A registered order of 10 at 100, brought to `stage`, with placement nonce 77 when `nonce`.
fn order_at(reg: &mut Registry, stage: Ack, nonce: bool) -> ClientOrderId {
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    if nonce {
        reg.placement_nonce_used(c, 77).unwrap();
    }
    match stage {
        Ack::Pending => {}
        Ack::Unknown => {
            reg.on_outcome(
                c,
                OrderOp::Place,
                &ItemRef {
                    idx: 0,
                    cid: None,
                    vid: None,
                },
                &SubmitOutcome::Unknown,
                MonoNs(1),
            )
            .unwrap();
            assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
        }
        Ack::AckedNoVid => ack(reg, c, None),
        Ack::AckedVid => ack(reg, c, Some(vid(&format!("v-{c:?}")))),
    }
    c
}

fn caps_with(refs: &[RefKind], cancel_before_ack: bool) -> OrderCaps {
    OrderCaps {
        cancel_refs: TagSet::of(refs),
        cancel_before_ack,
        ..order_caps()
    }
}

/// Every subset of the reference kinds.
fn subsets() -> Vec<Vec<RefKind>> {
    let all = [RefKind::Venue, RefKind::Client, RefKind::PlacementNonce];
    (0..8u8)
        .map(|mask| {
            all.iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, k)| *k)
                .collect()
        })
        .collect()
}

/// The single cancel a permitted command holds.
fn single(cmd: &PermittedCommand) -> &CancelOrder {
    match cmd.command() {
        VenueCommand::Cancel(cancel) => cancel,
        other => panic!("expected a single cancel, got {other:?}"),
    }
}

// ---- the cancel's reference ----

#[test]
fn a_cancel_names_its_order_by_the_reference_design_4_9_orders_for_every_combination() {
    let stages = [Ack::Pending, Ack::Unknown, Ack::AckedNoVid, Ack::AckedVid];
    let mut cases = 0;
    for stage in stages {
        for refs in subsets() {
            for before_ack in [false, true] {
                for nonce in [false, true] {
                    cases += 1;
                    let caps = caps_with(&refs, before_ack);
                    let has = |k| refs.contains(&k);
                    let mut reg = registry();
                    let c = order_at(&mut reg, stage, nonce);
                    let acked = matches!(stage, Ack::AckedNoVid | Ack::AckedVid);
                    let known_vid = reg.get(c).unwrap().vid().cloned();
                    // The oracle: design §4.9's order. The venue id when known and declared;
                    // else the client id when declared and the order is acknowledged or the
                    // venue cancels before the acknowledgement; else the placement nonce when
                    // declared and recorded; else wait for the acknowledgement. A command
                    // always carries our client id, which a codec picks before the nonce, so a
                    // venue declaring both waits for the acknowledgement instead (FBC-03fi).
                    let expected = if known_vid.is_some() && has(RefKind::Venue) {
                        Some(RefKind::Venue)
                    } else if has(RefKind::Client) && (acked || before_ack) {
                        Some(RefKind::Client)
                    } else if has(RefKind::PlacementNonce) && nonce && !has(RefKind::Client) {
                        Some(RefKind::PlacementNonce)
                    } else {
                        None
                    };
                    let what =
                        format!("{stage:?} refs {refs:?} before_ack {before_ack} nonce {nonce}");
                    let choice = reg.cancellable(c).unwrap().cancel(&caps);
                    match (expected, choice) {
                        (Some(kind), CancelChoice::Send(cmd)) => {
                            let cancel = single(&cmd);
                            let target = match &known_vid {
                                Some(v) => OrderRef::Both(c, v.clone()),
                                None => OrderRef::Client(c),
                            };
                            assert_eq!(
                                cancel,
                                &CancelOrder {
                                    target,
                                    inst: INST,
                                    side: Side::Buy,
                                    placement_nonce: nonce.then_some(77),
                                },
                                "{what}"
                            );
                            // The codec, choosing from the same command and the same caps,
                            // names the order by the same reference.
                            let chosen = cancel.reference(caps.cancel_refs).unwrap();
                            let got = match chosen {
                                fbc_core::ChosenRef::Venue(_) => RefKind::Venue,
                                fbc_core::ChosenRef::Client(_) => RefKind::Client,
                                fbc_core::ChosenRef::PlacementNonce(n) => {
                                    assert_eq!(n, 77);
                                    RefKind::PlacementNonce
                                }
                            };
                            assert_eq!(got, kind, "{what}");
                            assert!(!reg.get(c).unwrap().cancel_awaits_ack(), "{what}");
                        }
                        (None, CancelChoice::AwaitAck) => {
                            assert!(reg.get(c).unwrap().cancel_awaits_ack(), "{what}");
                            // Nothing changed since: not due yet.
                            assert_eq!(reg.cancels_due(&caps), vec![], "{what}");
                        }
                        (expected, choice) => {
                            panic!("{what}: expected {expected:?}, got {choice:?}")
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cases, 4 * 8 * 2 * 2);
}

#[test]
fn a_cancel_waiting_for_the_acknowledgement_is_due_the_moment_it_lands() {
    // Cancels by venue id only: an order the venue has not named waits.
    let caps = caps_with(&[RefKind::Venue], false);
    let mut reg = registry();
    let c = order_at(&mut reg, Ack::Pending, false);
    let quiet = order_at(&mut reg, Ack::Pending, false);
    assert_eq!(
        reg.cancellable(c).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    // While its cancel waits, the order is not Live, so no amend overtakes the cancel.
    assert!(reg.get(c).unwrap().cancel_awaits_ack());
    assert_eq!(reg.cancels_due(&caps), vec![]);

    // The acknowledgement lands with the venue's id: the cancel is due, and goes by it.
    ack(&mut reg, c, Some(vid("c1")));
    ack(&mut reg, quiet, Some(vid("q1")));
    assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::IntentPending(c));
    assert_eq!(reg.cancels_due(&caps), vec![c]);
    let CancelChoice::Send(cmd) = reg.cancellable(c).unwrap().cancel(&caps) else {
        panic!("the cancel is due")
    };
    assert_eq!(single(&cmd).target, OrderRef::Both(c, vid("c1")));
    // Built, it no longer waits.
    assert_eq!(reg.cancels_due(&caps), vec![]);
    assert!(!reg.get(c).unwrap().cancel_awaits_ack());

    // Cancels by client id, none before the acknowledgement: an acknowledgement without the
    // venue's id is enough.
    let caps = caps_with(&[RefKind::Client], false);
    let d = order_at(&mut reg, Ack::Unknown, false);
    assert_eq!(
        reg.cancellable(d).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    ack(&mut reg, d, None);
    assert_eq!(reg.cancels_due(&caps), vec![d]);
    // Sending the cancel clears the wait too.
    assert_eq!(reg.cancel_sent(d, RpcId(9), MonoNs(2)), Ok(true));
    assert_eq!(reg.cancels_due(&caps), vec![]);

    // An order that ends while its cancel waits is not due.
    let e = order_at(&mut reg, Ack::Pending, false);
    assert_eq!(
        reg.cancellable(e).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    let mut ended = update(Some(e), common::canceled(), 0);
    ended.vid = Some(vid("e1"));
    reg.apply_update(
        &ended,
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert!(!reg.get(e).unwrap().cancel_awaits_ack());
    assert_eq!(reg.cancels_due(&caps), vec![]);
}

#[test]
fn an_acknowledgement_that_names_no_usable_reference_leaves_the_cancel_waiting() {
    // Cancels by venue id only, and the acknowledgement carries no venue id.
    let caps = caps_with(&[RefKind::Venue], false);
    let mut reg = registry();
    let c = order_at(&mut reg, Ack::Pending, false);
    assert_eq!(
        reg.cancellable(c).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    ack(&mut reg, c, None);
    assert_eq!(reg.cancels_due(&caps), vec![]);
    // The venue's id arrives on an update: now it is due.
    let mut open = update(Some(c), VenueOrderState::Open, 0);
    open.vid = Some(vid("c9"));
    reg.apply_update(
        &open,
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert_eq!(reg.cancels_due(&caps), vec![c]);
}

#[test]
fn a_venue_declaring_client_and_nonce_cancels_waits_for_the_acknowledgement_rather_than_send_a_client_id_too_early()
 {
    // The command carries our client id, which a codec picks before the nonce, and the venue
    // refuses a client-id cancel before the acknowledgement: sending would be refused, so the
    // OMS waits (FBC-03fi would let the command carry the nonce alone).
    let caps = caps_with(&[RefKind::Client, RefKind::PlacementNonce], false);
    let mut reg = registry();
    let c = order_at(&mut reg, Ack::Pending, true);
    assert_eq!(
        reg.cancellable(c).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    // With nonces only, the nonce names it before the acknowledgement.
    let caps = caps_with(&[RefKind::PlacementNonce], false);
    let CancelChoice::Send(cmd) = reg.cancellable(c).unwrap().cancel(&caps) else {
        panic!("a nonce cancel goes before the acknowledgement")
    };
    assert_eq!(
        single(&cmd).reference(caps.cancel_refs),
        Some(fbc_core::ChosenRef::PlacementNonce(77))
    );
}

#[test]
fn a_placement_nonce_is_recorded_once() {
    let mut reg = registry();
    let c = reg.insert(placement(cid(), 100, 1)).unwrap().cid();
    assert_eq!(reg.get(c).unwrap().placement_nonce(), None);
    assert_eq!(reg.placement_nonce_used(c, 5), Ok(()));
    // The same nonce again changes nothing; another is refused.
    assert_eq!(reg.placement_nonce_used(c, 5), Ok(()));
    assert_eq!(
        reg.placement_nonce_used(c, 6),
        Err(OmsError::NonceRecorded(c))
    );
    assert_eq!(reg.get(c).unwrap().placement_nonce(), Some(5));
    assert!(
        OmsError::NonceRecorded(c)
            .to_string()
            .contains("another placement nonce")
    );
    let stranger = cid();
    assert_eq!(
        reg.placement_nonce_used(stranger, 1),
        Err(OmsError::UnknownCid(stranger))
    );
    assert_eq!(
        reg.amend_sent(stranger, Ticks(1), lots(1), RpcId(1), MonoNs(0)),
        Err(OmsError::UnknownCid(stranger))
    );
    assert_eq!(
        reg.cancel_sent(stranger, RpcId(1), MonoNs(0)),
        Err(OmsError::UnknownCid(stranger))
    );
}

// ---- the Cancellable permit ----

#[test]
fn every_order_that_is_not_terminal_is_cancellable_and_no_other() {
    let caps = caps_with(&[RefKind::Venue, RefKind::Client], true);
    let mut reg = registry();
    let pending = order_at(&mut reg, Ack::Pending, false);
    let unknown = order_at(&mut reg, Ack::Unknown, false);
    let amending = order_at(&mut reg, Ack::AckedVid, false);
    assert_eq!(
        reg.amend_sent(amending, Ticks(101), lots(10), RpcId(1), MonoNs(1)),
        Ok(true)
    );
    let cancelling = order_at(&mut reg, Ack::AckedVid, false);
    assert_eq!(reg.cancel_sent(cancelling, RpcId(2), MonoNs(1)), Ok(true));
    for c in [pending, unknown, amending, cancelling] {
        let permit = reg.cancellable(c).unwrap();
        assert_eq!(permit.order().cid(), c);
        assert!(matches!(permit.cancel(&caps), CancelChoice::Send(_)));
    }

    let ended = order_at(&mut reg, Ack::AckedVid, false);
    reg.apply_update(
        &update(Some(ended), common::canceled(), 0),
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert_eq!(
        reg.cancellable(ended).unwrap_err(),
        PermitRefusal::Terminal(ended)
    );
    // A terminal order records no command sent.
    assert_eq!(reg.cancel_sent(ended, RpcId(3), MonoNs(2)), Ok(false));
    let stranger = cid();
    assert_eq!(
        reg.cancellable(stranger).unwrap_err(),
        PermitRefusal::UnknownCid(stranger)
    );
}

// ---- I4: foreign-namespace and non-canonical orders ----

#[test]
fn a_foreign_namespace_order_is_never_cancelled_individually_and_its_fill_moves_nothing() {
    let caps = caps_with(&[RefKind::Venue, RefKind::Client], true);
    let mut reg = registry();
    let ours = order_at(&mut reg, Ack::AckedVid, false);
    let our_vid = reg.get(ours).unwrap().vid().cloned().unwrap();

    // Another engine's order on the account, and another system's: no permit, whatever venue
    // id they carry, even one of ours.
    let foreign = CidMatch::Foreign(Namespace::new(9));
    for seen_vid in [None, Some(vid("theirs")), Some(our_vid.clone())] {
        assert_eq!(
            reg.cancellable_seen(Some(foreign), seen_vid.as_ref())
                .unwrap_err(),
            PermitRefusal::Foreign(Namespace::new(9))
        );
        assert_eq!(
            reg.cancellable_seen(Some(CidMatch::Unparseable), seen_vid.as_ref())
                .unwrap_err(),
            PermitRefusal::NotCanonical
        );
    }
    // Ours by client id or, with none echoed, by venue id: a permit.
    let permit = reg
        .cancellable_seen(Some(CidMatch::Ours(ours)), None)
        .unwrap();
    assert_eq!(permit.order().cid(), ours);
    let permit = reg.cancellable_seen(None, Some(&our_vid)).unwrap();
    assert!(matches!(permit.cancel(&caps), CancelChoice::Send(_)));
    // An order the registry does not hold (an orphan, I7's): no permit here.
    assert_eq!(
        reg.cancellable_seen(Some(CidMatch::Ours(cid())), None)
            .unwrap_err(),
        PermitRefusal::Untracked
    );
    assert_eq!(
        reg.cancellable_seen(None, Some(&vid("nobody")))
            .unwrap_err(),
        PermitRefusal::Untracked
    );
    assert_eq!(
        reg.cancellable_seen(None, None).unwrap_err(),
        PermitRefusal::Untracked
    );

    // Its fills are flagged and move nothing.
    let mut ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(60),
            max_entries: 16,
        },
        WallNs(0),
    )
    .unwrap();
    for (cid_match, routed) in [
        (foreign, FillRouted::Foreign(Namespace::new(9))),
        (CidMatch::Unparseable, FillRouted::NotCanonical),
    ] {
        let mut f = fill(None, ident(&format!("f-{routed:?}")), Side::Buy, 3, false);
        f.cid = Some(cid_match);
        let Admission::Apply(accepted) = ledger.admit(&f, None, MonoNs(1)) else {
            panic!("a new fill is admitted")
        };
        assert_eq!(reg.apply_fill(accepted), Ok(routed));
        assert_eq!(reg.inventory(INST), SignedLots(0));
        assert_eq!(reg.get(ours).unwrap().filled(), lots(0));
    }
}

// ---- cancel-many ----

/// A Paradex-like venue (no real venue's values): single cancels by venue or client id,
/// batch cancels by venue id only, `max_items` per batch.
fn batch_caps(max_items: u16) -> OrderCaps {
    OrderCaps {
        batch_cancel: Some(CancelBatch {
            max_items,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..caps_with(&[RefKind::Venue, RefKind::Client], false)
    }
}

/// The items of the cancel-many a permitted command holds.
fn many(cmd: &PermittedCommand) -> &[CancelOrder] {
    match cmd.command() {
        VenueCommand::CancelMany(items) => items,
        other => panic!("expected a cancel-many, got {other:?}"),
    }
}

#[test]
fn a_cancel_many_item_without_a_batch_declared_reference_goes_as_a_single_cancel() {
    let caps = batch_caps(2);
    let mut reg = registry();
    let a = order_at(&mut reg, Ack::AckedVid, false);
    let b = order_at(&mut reg, Ack::AckedVid, false);
    let c = order_at(&mut reg, Ack::AckedVid, false);
    // Acknowledged without the venue's id: only its client id names it, which the batch does
    // not take, so it goes alone, by client id, carrying its market.
    let by_client = order_at(&mut reg, Ack::AckedNoVid, false);
    // Not acknowledged, and the venue takes no cancel before: it waits.
    let waiting = order_at(&mut reg, Ack::Pending, false);
    // On another market.
    let mut other = placement(cid(), 50, 1);
    other.inst = InstrumentId::new(2);
    let elsewhere = reg.insert(other).unwrap().cid();
    ack(&mut reg, elsewhere, Some(vid("x1")));
    // Terminal, and unknown to the registry: refused.
    let ended = order_at(&mut reg, Ack::AckedVid, false);
    reg.apply_update(
        &update(Some(ended), common::canceled(), 0),
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    let stranger = cid();

    let plan = reg.cancel_many(
        &[a, by_client, b, waiting, elsewhere, c, ended, stranger, a],
        &caps,
    );
    let vid_of = |c: ClientOrderId| reg.get(c).unwrap().vid().cloned().unwrap();
    assert_eq!(plan.commands.len(), 4, "{plan:?}");
    // Market 1's batchable items in two batches of at most two, in the order given.
    let first: Vec<_> = many(&plan.commands[0])
        .iter()
        .map(|i| i.target.clone())
        .collect();
    assert_eq!(
        first,
        vec![OrderRef::Both(a, vid_of(a)), OrderRef::Both(b, vid_of(b))]
    );
    let second: Vec<_> = many(&plan.commands[1])
        .iter()
        .map(|i| i.target.clone())
        .collect();
    assert_eq!(second, vec![OrderRef::Both(c, vid_of(c))]);
    // Market 2's in its own batch.
    let third = many(&plan.commands[2]);
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].inst, InstrumentId::new(2));
    // Every batch item names a reference the batch declares.
    for cmd in &plan.commands[..3] {
        for item in many(cmd) {
            assert!(item.reference(caps.batch_cancel.unwrap().refs).is_some());
        }
    }
    // The client-id item as a single cancel carrying its market.
    let alone = single(&plan.commands[3]);
    assert_eq!(alone.target, OrderRef::Client(by_client));
    assert_eq!(alone.inst, INST);
    assert_eq!(
        alone.reference(caps.cancel_refs),
        Some(fbc_core::ChosenRef::Client(by_client))
    );
    assert_eq!(alone.reference(caps.batch_cancel.unwrap().refs), None);
    assert_eq!(plan.awaiting_ack, vec![waiting]);
    assert!(reg.get(waiting).unwrap().cancel_awaits_ack());
    assert_eq!(
        plan.refused,
        vec![
            (ended, PermitRefusal::Terminal(ended)),
            (stranger, PermitRefusal::UnknownCid(stranger)),
        ]
    );
}

#[test]
fn without_a_batch_cancel_every_item_goes_as_a_single_cancel() {
    let mut reg = registry();
    let a = order_at(&mut reg, Ack::AckedVid, false);
    let b = order_at(&mut reg, Ack::AckedVid, false);
    for caps in [
        caps_with(&[RefKind::Venue], false),
        // A batch of no items is no batch.
        batch_caps(0),
    ] {
        let plan = reg.cancel_many(&[a, b], &caps);
        let targets: Vec<_> = plan
            .commands
            .iter()
            .map(|c| single(c).target.client())
            .collect();
        assert_eq!(targets, vec![Some(a), Some(b)]);
        assert!(plan.awaiting_ack.is_empty() && plan.refused.is_empty());
    }
    // A batch item built clears a wait for the acknowledgement.
    let w = order_at(&mut reg, Ack::Pending, false);
    let caps = batch_caps(4);
    assert_eq!(
        reg.cancellable(w).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    ack(&mut reg, w, Some(vid("w1")));
    let plan = reg.cancel_many(&[w], &caps);
    assert_eq!(many(&plan.commands[0]).len(), 1);
    assert!(!reg.get(w).unwrap().cancel_awaits_ack());
}

// ---- the Live permit and the amend ----

fn amend_caps(refs: &[RefKind], when_partially_filled: bool) -> AmendCaps {
    AmendCaps {
        refs: TagSet::of(refs),
        price: true,
        qty: true,
        flags: false,
        when_partially_filled,
        reject_keeps_original: true,
        keeps_venue_id: true,
        ack: AmendAck::ReplacedEvent,
        qty_semantics: AmendQty::TotalIncludingFilled,
        keeps_priority: None,
    }
}

fn with_amend(amend: AmendCaps) -> OrderCaps {
    OrderCaps {
        amend: Some(amend),
        ..order_caps()
    }
}

/// Fills `qty` of our order `c` through the ledger.
fn fill_of(reg: &mut Registry, ledger: &mut FillLedger, c: ClientOrderId, fid: &str, qty: i64) {
    let f = fill(Some(c), ident(fid), Side::Buy, qty, false);
    let Admission::Apply(accepted) = ledger.admit(&f, None, MonoNs(1)) else {
        panic!("a new fill is admitted")
    };
    reg.apply_fill(accepted).unwrap();
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(60),
            max_entries: 16,
        },
        WallNs(0),
    )
    .unwrap()
}

#[test]
fn an_amend_is_built_from_the_record_and_carries_the_filled_quantity() {
    let caps = with_amend(amend_caps(&[RefKind::Venue], true));
    let mut reg = registry();
    let mut ledger = ledger();
    let c = order_at(&mut reg, Ack::AckedVid, false);
    fill_of(&mut reg, &mut ledger, c, "a", 3);
    let live = reg.live(c).unwrap();
    assert_eq!(live.order().state(), OrdState::PartiallyFilled);
    let cmd = live.amend(&caps, Ticks(101), lots(12), false).unwrap();
    let placed = placement(c, 100, 10);
    assert_eq!(
        cmd.command(),
        &VenueCommand::Amend(AmendOrder {
            target: OrderRef::Both(c, reg.get(c).unwrap().vid().cloned().unwrap()),
            inst: INST,
            side: Side::Buy,
            tif: placed.tif,
            channel: placed.channel,
            post_only: placed.post_only,
            reduce_only: placed.reduce_only,
            reducing: false,
            px: Ticks(101),
            qty: lots(12),
            cum_filled: lots(3),
        })
    );
}

#[test]
fn an_amend_is_refused_for_a_partly_filled_order_where_the_caps_forbid_it() {
    let caps = with_amend(amend_caps(&[RefKind::Venue], false));
    let mut reg = registry();
    let mut ledger = ledger();
    let c = order_at(&mut reg, Ack::AckedVid, false);
    // Unfilled, it amends.
    assert!(
        reg.live(c)
            .unwrap()
            .amend(&caps, Ticks(101), lots(10), false)
            .is_ok()
    );
    // Never submitted: withdrawn, so the order can be amended again.
    assert_eq!(reg.amend_not_submitted(c), Ok(true));
    fill_of(&mut reg, &mut ledger, c, "p", 1);
    assert_eq!(
        reg.live(c)
            .unwrap()
            .amend(&caps, Ticks(101), lots(10), false),
        Err(AmendRefusal::PartiallyFilled)
    );
}

#[test]
fn an_amend_is_refused_for_a_reference_amend_caps_do_not_declare() {
    let mut reg = registry();
    // Acknowledged without the venue's id: only the client id names it.
    let c = order_at(&mut reg, Ack::AckedNoVid, false);
    let by_venue = with_amend(amend_caps(&[RefKind::Venue], true));
    assert_eq!(
        reg.live(c)
            .unwrap()
            .amend(&by_venue, Ticks(101), lots(10), false),
        Err(AmendRefusal::NoDeclaredReference)
    );
    // An amend carries no placement nonce, so declaring nonces does not help.
    let by_nonce = with_amend(amend_caps(&[RefKind::PlacementNonce], true));
    reg.placement_nonce_used(c, 3).unwrap();
    assert_eq!(
        reg.live(c)
            .unwrap()
            .amend(&by_nonce, Ticks(101), lots(10), false),
        Err(AmendRefusal::NoDeclaredReference)
    );
    // Declared by client id, it amends by client id.
    let by_client = with_amend(amend_caps(&[RefKind::Client], true));
    let cmd = reg
        .live(c)
        .unwrap()
        .amend(&by_client, Ticks(101), lots(10), false)
        .unwrap();
    let VenueCommand::Amend(amend) = cmd.command() else {
        panic!("an amend")
    };
    assert_eq!(
        amend.reference(by_client.amend.as_ref().unwrap()),
        Some(fbc_core::ChosenRef::Client(c))
    );
}

#[test]
fn an_amend_is_refused_where_the_venue_cannot_make_it() {
    let mut reg = registry();
    let mut ledger = ledger();
    let c = order_at(&mut reg, Ack::AckedVid, false);
    // Each amend built is withdrawn, never submitted, so the next can be built.
    let amend = |reg: &mut Registry, caps: &OrderCaps, px: i64, qty: i64| {
        let built = reg
            .live(c)
            .unwrap()
            .amend(caps, Ticks(px), lots(qty), false);
        if built.is_ok() {
            assert_eq!(reg.amend_not_submitted(c), Ok(true));
        }
        built
    };
    assert_eq!(
        amend(&mut reg, &order_caps(), 101, 10),
        Err(AmendRefusal::NotAmendable)
    );
    let fixed_px = with_amend(AmendCaps {
        price: false,
        ..amend_caps(&[RefKind::Venue], true)
    });
    assert_eq!(
        amend(&mut reg, &fixed_px, 101, 10),
        Err(AmendRefusal::PriceNotAmendable)
    );
    assert!(amend(&mut reg, &fixed_px, 100, 12).is_ok());
    let fixed_qty = with_amend(AmendCaps {
        qty: false,
        ..amend_caps(&[RefKind::Venue], true)
    });
    assert_eq!(
        amend(&mut reg, &fixed_qty, 100, 12),
        Err(AmendRefusal::QtyNotAmendable)
    );
    assert!(amend(&mut reg, &fixed_qty, 101, 10).is_ok());
    // A total at or below the filled quantity leaves nothing to rest: a cancel, not an amend.
    let caps = with_amend(amend_caps(&[RefKind::Venue], true));
    fill_of(&mut reg, &mut ledger, c, "n", 4);
    assert_eq!(
        amend(&mut reg, &caps, 100, 4),
        Err(AmendRefusal::NothingToRest)
    );
    assert!(amend(&mut reg, &caps, 100, 5).is_ok());

    // A resting market order is not amended.
    let mut market = placement(cid(), 100, 10);
    market.kind = OrderKind::Market;
    let m = reg.insert(market).unwrap().cid();
    ack(&mut reg, m, Some(vid("m1")));
    assert_eq!(
        reg.live(m)
            .unwrap()
            .amend(&caps, Ticks(100), lots(10), false),
        Err(AmendRefusal::NotLimit)
    );
}

#[test]
fn only_a_resting_order_with_nothing_in_flight_is_live() {
    let mut reg = registry();
    let pending = order_at(&mut reg, Ack::Pending, false);
    let unknown = order_at(&mut reg, Ack::Unknown, false);
    assert_eq!(
        reg.live(pending).unwrap_err(),
        PermitRefusal::NotResting(pending, OrdState::PendingNew)
    );
    assert_eq!(
        reg.live(unknown).unwrap_err(),
        PermitRefusal::NotResting(unknown, OrdState::Unknown)
    );
    let amending = order_at(&mut reg, Ack::AckedVid, false);
    reg.amend_sent(amending, Ticks(101), lots(10), RpcId(1), MonoNs(1))
        .unwrap();
    assert_eq!(
        reg.live(amending).unwrap_err(),
        PermitRefusal::IntentPending(amending)
    );
    let cancelling = order_at(&mut reg, Ack::AckedVid, false);
    reg.cancel_sent(cancelling, RpcId(2), MonoNs(1)).unwrap();
    assert_eq!(
        reg.live(cancelling).unwrap_err(),
        PermitRefusal::IntentPending(cancelling)
    );
    let ended = order_at(&mut reg, Ack::AckedVid, false);
    reg.apply_update(
        &update(Some(ended), common::canceled(), 0),
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert_eq!(reg.live(ended).unwrap_err(), PermitRefusal::Terminal(ended));
    let stranger = cid();
    assert_eq!(
        reg.live(stranger).unwrap_err(),
        PermitRefusal::UnknownCid(stranger)
    );
    let open = order_at(&mut reg, Ack::AckedNoVid, false);
    assert_eq!(reg.live(open).unwrap().order().state(), OrdState::Open);
}

// ---- review fixes ----

#[test]
fn a_cancel_while_an_amend_that_replaces_the_venue_id_is_in_flight_does_not_name_the_old_id() {
    // Codex r4186718675: the venue gives the amended order a new id, and the amend is on its
    // way, so the record's venue id may already be retired.
    let replacing = |refs: &[RefKind]| OrderCaps {
        amend: Some(AmendCaps {
            keeps_venue_id: false,
            ..amend_caps(&[RefKind::Venue], true)
        }),
        batch_cancel: Some(CancelBatch {
            max_items: 4,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..caps_with(refs, false)
    };
    let mut reg = registry();
    let c = order_at(&mut reg, Ack::AckedVid, false);
    let old = reg.get(c).unwrap().vid().cloned().unwrap();
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(1), MonoNs(1))
        .unwrap();

    // By venue id only: the cancel waits, single or in a batch.
    let caps = replacing(&[RefKind::Venue]);
    assert_eq!(
        reg.cancellable(c).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    let plan = reg.cancel_many(&[c], &caps);
    assert!(plan.commands.is_empty(), "{plan:?}");
    assert_eq!(plan.awaiting_ack, vec![c]);
    // By client id too: the client id names it, not the old venue id.
    let both = replacing(&[RefKind::Venue, RefKind::Client]);
    let CancelChoice::Send(cmd) = reg.cancellable(c).unwrap().cancel(&both) else {
        panic!("the client id names the order")
    };
    assert_eq!(single(&cmd).target, OrderRef::Client(c));
    let plan = reg.cancel_many(&[c], &both);
    assert_eq!(single(&plan.commands[0]).target, OrderRef::Client(c));

    // The amend confirmed under its new id: the cancel by venue id is due, naming the new id.
    assert_eq!(
        reg.cancellable(c).unwrap().cancel(&caps),
        CancelChoice::AwaitAck
    );
    let mut amended = update(
        Some(c),
        VenueOrderState::Amended {
            new_vid: Some(vid("new")),
        },
        0,
    );
    amended.vid = Some(old);
    amended.px = Some(Ticks(101));
    amended.qty = Some(lots(10));
    reg.apply_update(
        &amended,
        OrderKey {
            venue: Some(5),
            ingest: 2,
        },
    );
    assert_eq!(reg.cancels_due(&caps), vec![c]);
    let CancelChoice::Send(cmd) = reg.cancellable(c).unwrap().cancel(&caps) else {
        panic!("due")
    };
    assert_eq!(single(&cmd).target, OrderRef::Both(c, vid("new")));

    // A venue that keeps the id across an amend names it while the amend is in flight.
    let d = order_at(&mut reg, Ack::AckedVid, false);
    reg.amend_sent(d, Ticks(101), lots(10), RpcId(3), MonoNs(1))
        .unwrap();
    let keeping = OrderCaps {
        amend: Some(amend_caps(&[RefKind::Venue], true)),
        ..caps_with(&[RefKind::Venue], false)
    };
    assert!(matches!(
        reg.cancellable(d).unwrap().cancel(&keeping),
        CancelChoice::Send(_)
    ));
}

#[test]
fn an_amend_the_planner_classifies_as_reducing_stays_safety_traffic() {
    // Codex r4186718683: the classification is the caller's, as on a placement.
    let caps = with_amend(amend_caps(&[RefKind::Venue], true));
    let mut reg = registry();
    let c = order_at(&mut reg, Ack::AckedVid, false);
    for reducing in [false, true] {
        let cmd = reg
            .live(c)
            .unwrap()
            .amend(&caps, Ticks(100), lots(8), reducing)
            .unwrap();
        assert_eq!(reg.amend_not_submitted(c), Ok(true));
        let VenueCommand::Amend(amend) = cmd.command() else {
            panic!("an amend")
        };
        assert_eq!(amend.reducing, reducing);
        assert_eq!(
            cmd.command().traffic_class(),
            if reducing {
                fbc_core::TrafficClass::Safety
            } else {
                fbc_core::TrafficClass::Normal
            }
        );
    }
}

#[test]
fn a_seen_order_whose_client_id_and_venue_id_name_different_orders_gets_no_permit() {
    // Codex r4186718692, as Registry::apply_fill flags such a fill.
    let mut reg = registry();
    let a = order_at(&mut reg, Ack::AckedVid, false);
    let b = order_at(&mut reg, Ack::AckedVid, false);
    let b_vid = reg.get(b).unwrap().vid().cloned().unwrap();
    let a_vid = reg.get(a).unwrap().vid().cloned().unwrap();
    assert_eq!(
        reg.cancellable_seen(Some(CidMatch::Ours(a)), Some(&b_vid))
            .unwrap_err(),
        PermitRefusal::Conflicting { cid: a, by_vid: b }
    );
    // Both naming the same order, or no venue id: a permit for it.
    for seen in [Some(&a_vid), None] {
        let permit = reg.cancellable_seen(Some(CidMatch::Ours(a)), seen).unwrap();
        assert_eq!(permit.order().cid(), a);
    }
}

#[test]
fn a_seen_order_naming_a_venue_id_its_record_has_not_learnt_gets_no_permit_until_it_has() {
    // Codex r4186908002: the record would cancel by an id it holds, or wait for one, while the
    // venue shows another. The event showing it is applied first, and the record learns it.
    let caps = caps_with(&[RefKind::Venue], false);
    let mut reg = registry();
    let a = order_at(&mut reg, Ack::Pending, false);
    let seen = vid("seen-a");
    assert_eq!(
        reg.cancellable_seen(Some(CidMatch::Ours(a)), Some(&seen))
            .unwrap_err(),
        PermitRefusal::Unlearned(a)
    );
    let mut open = update(Some(a), VenueOrderState::Open, 0);
    open.vid = Some(seen.clone());
    reg.apply_update(
        &open,
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    let CancelChoice::Send(cmd) = reg
        .cancellable_seen(Some(CidMatch::Ours(a)), Some(&seen))
        .unwrap()
        .cancel(&caps)
    else {
        panic!("the venue id is learnt")
    };
    assert_eq!(single(&cmd).target, OrderRef::Both(a, seen));
}

#[test]
fn an_amend_while_an_earlier_one_that_replaces_the_venue_id_is_unconfirmed_does_not_name_the_old_id()
 {
    // Codex r4186908014: an amend replaced in flight by a cancel the venue refused may still
    // have given the order a new id; the next amend names it by client id, or is refused.
    let caps = |refs: &[RefKind]| {
        with_amend(AmendCaps {
            keeps_venue_id: false,
            ..amend_caps(refs, true)
        })
    };
    let mut reg = registry();
    let c = order_at(&mut reg, Ack::AckedVid, false);
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(1), MonoNs(1))
        .unwrap();
    reg.cancel_sent(c, RpcId(2), MonoNs(2)).unwrap();
    let refused = SubmitOutcome::Rejected(fbc_core::Reject {
        kind: fbc_core::RejectKind::Margin,
        venue_code: None,
        raw: "refused".into(),
    });
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: None,
    };
    reg.on_outcome(c, OrderOp::Cancel(RpcId(2)), &item, &refused, MonoNs(3))
        .unwrap();
    assert!(reg.get(c).unwrap().amend_unconfirmed());
    assert_eq!(
        reg.live(c)
            .unwrap()
            .amend(&caps(&[RefKind::Venue]), Ticks(102), lots(10), false),
        Err(AmendRefusal::NoDeclaredReference)
    );
    let cmd = reg
        .live(c)
        .unwrap()
        .amend(
            &caps(&[RefKind::Venue, RefKind::Client]),
            Ticks(102),
            lots(10),
            false,
        )
        .unwrap();
    let VenueCommand::Amend(amend) = cmd.command() else {
        panic!("an amend")
    };
    assert_eq!(amend.target, OrderRef::Client(c));
    assert_eq!(reg.amend_not_submitted(c), Ok(true));
    // A venue that keeps the id names it.
    let keeping = with_amend(amend_caps(&[RefKind::Venue], true));
    assert!(
        reg.live(c)
            .unwrap()
            .amend(&keeping, Ticks(102), lots(10), false)
            .is_ok()
    );
}

#[test]
fn an_amend_confirmed_without_its_new_venue_id_leaves_the_old_id_out_of_every_later_command() {
    // Codex r4187102109: on a venue whose amend gives a new id, a confirmation that does not
    // echo it retires the old id without teaching the new one.
    let replacing = |refs: &[RefKind]| OrderCaps {
        amend: Some(AmendCaps {
            keeps_venue_id: false,
            ..amend_caps(refs, true)
        }),
        ..caps_with(refs, false)
    };
    let mut reg = registry();
    let c = order_at(&mut reg, Ack::AckedVid, false);
    let old = reg.get(c).unwrap().vid().cloned().unwrap();
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(1), MonoNs(1))
        .unwrap();
    let mut amended = update(Some(c), VenueOrderState::Amended { new_vid: None }, 0);
    amended.vid = Some(old.clone());
    amended.px = Some(Ticks(101));
    amended.qty = Some(lots(10));
    reg.apply_update(
        &amended,
        OrderKey {
            venue: Some(5),
            ingest: 2,
        },
    );
    let rec = reg.get(c).unwrap();
    assert!(!rec.amend_unconfirmed());
    assert!(rec.vid_retired());

    // By venue id only: the cancel waits and the amend is refused, rather than name `old`.
    let by_venue = replacing(&[RefKind::Venue]);
    assert_eq!(
        reg.live(c)
            .unwrap()
            .amend(&by_venue, Ticks(102), lots(10), false),
        Err(AmendRefusal::NoDeclaredReference)
    );
    assert_eq!(
        reg.cancellable(c).unwrap().cancel(&by_venue),
        CancelChoice::AwaitAck
    );
    // By client id too: the client id names it.
    let both = replacing(&[RefKind::Venue, RefKind::Client]);
    let CancelChoice::Send(cmd) = reg.cancellable(c).unwrap().cancel(&both) else {
        panic!("the client id names the order")
    };
    assert_eq!(single(&cmd).target, OrderRef::Client(c));
    // A venue that keeps the id across amends still names it.
    let keeping = OrderCaps {
        amend: Some(amend_caps(&[RefKind::Venue], true)),
        ..caps_with(&[RefKind::Venue], false)
    };
    let CancelChoice::Send(cmd) = reg.cancellable(c).unwrap().cancel(&keeping) else {
        panic!("the kept id names the order")
    };
    assert_eq!(single(&cmd).target, OrderRef::Both(c, old));
}
