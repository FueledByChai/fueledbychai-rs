//! Decision 0005's I1 and I2 for order updates, property-tested (decision 0037).
//!
//! I1: whenever a terminal update is present, the final state does not depend on the order the
//! cumulative order updates of one order arrive in, on duplicates, or on ties in their ordering
//! keys. Each case generates one order's history as a venue reports it (cumulative fills that
//! never fall, ordering keys that never fall and often tie, amends that may issue a new venue
//! id, one terminal update last), delivers every update one to three times in a shuffled order,
//! and compares the record with the one the history in order leaves.
//!
//! I2: a terminal order never leaves the terminal state, and a terminal update always applies
//! unless it carries a superseded venue id. Each case plays an arbitrary sequence of order
//! updates (any state, any ordering key, venue ids from a small pool), item outcomes and sent
//! intents at a fresh order and checks every step.

mod common;

use common::{cid, lots, placement, rejected, update, vid};
use fbc_core::{
    AckLevel, CancelReason, Lots, MonoNs, NotSentReason, OrderUpdate, Reject, RejectKind, RpcId,
    SubmitOutcome, TerminalHint, Ticks, VenueOrderId, VenueOrderState,
};
use fbc_oms::{Applied, OrdState, OrderKey, OrderOp, OrderRecord, TerminalKind};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

/// Fixed cases and seed, so the check and its coverage are the same on every run and machine
/// (decision 0037); a failure names its minimal case.
fn config(cases: u32, seed: u64) -> ProptestConfig {
    ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// How a venue names its orders on its events.
#[derive(Copy, Clone, Debug)]
enum VidMode {
    /// No venue id on any event.
    Absent,
    /// The same venue id on every event; an amend keeps it.
    Kept,
    /// Every event names the order's current venue id, and an amend issues a new one.
    Replaced,
}

#[derive(Copy, Clone, Debug)]
enum End {
    Filled,
    Canceled(CancelReason),
    Rejected(RejectKind),
    Expired,
}

#[derive(Clone, Debug)]
struct Step {
    amend: bool,
    cum_up: i64,
    key_up: u64,
    states_px_qty: bool,
    new_px: i64,
    new_qty: i64,
}

#[derive(Clone, Debug)]
struct History {
    steps: Vec<Step>,
    end: End,
    end_cum_up: i64,
    end_key_up: u64,
    end_states_px_qty: bool,
    venue_keys: bool,
    vids: VidMode,
}

/// One order's updates in the order the venue produced them, each with its venue key.
struct Produced {
    updates: Vec<(OrderUpdate, Option<u64>)>,
    end: OrdState,
    end_cum: Lots,
    end_vid: Option<VenueOrderId>,
    end_px_qty: Option<(Ticks, Lots)>,
}

fn venue_id(n: usize) -> VenueOrderId {
    vid(&format!("v{n}"))
}

fn produce(h: &History, cid: fbc_core::ClientOrderId) -> Produced {
    let (mut cum, mut key, mut vid_n, mut px, mut qty) = (0i64, 0u64, 0usize, 100i64, 10i64);
    let current = |vid_n: usize| match h.vids {
        VidMode::Absent => None,
        VidMode::Kept | VidMode::Replaced => Some(venue_id(vid_n)),
    };
    let mut updates = Vec::new();
    for step in &h.steps {
        cum += step.cum_up;
        key += step.key_up;
        let mut u = update(Some(cid), VenueOrderState::Open, 0);
        u.vid = current(vid_n);
        if step.amend {
            px = step.new_px;
            qty = step.new_qty.max(cum + 1);
            let new_vid = match h.vids {
                VidMode::Replaced => {
                    vid_n += 1;
                    Some(venue_id(vid_n))
                }
                VidMode::Absent | VidMode::Kept => None,
            };
            u.state = VenueOrderState::Amended { new_vid };
        }
        qty = qty.max(cum + 1);
        u.cum_filled = lots(cum);
        if step.states_px_qty {
            (u.px, u.qty) = (Some(Ticks(px)), Some(lots(qty)));
        }
        updates.push((u, h.venue_keys.then_some(key)));
    }
    cum += h.end_cum_up;
    key += h.end_key_up;
    qty = qty.max(cum);
    let (state, end) = match h.end {
        End::Filled => {
            cum = qty;
            (VenueOrderState::Filled, TerminalKind::Filled)
        }
        End::Canceled(r) => (VenueOrderState::Canceled(r), TerminalKind::Canceled(r)),
        End::Rejected(k) => (rejected(k), TerminalKind::Rejected(k)),
        End::Expired => (VenueOrderState::Expired, TerminalKind::Expired),
    };
    let mut u = update(Some(cid), state, cum);
    u.vid = current(vid_n);
    if h.end_states_px_qty {
        (u.px, u.qty) = (Some(Ticks(px)), Some(lots(qty)));
    }
    let end_px_qty = h.end_states_px_qty.then(|| (Ticks(px), lots(qty)));
    let end_vid = u.vid.clone();
    updates.push((u, h.venue_keys.then_some(key)));
    Produced {
        updates,
        end: OrdState::Terminal(end),
        end_cum: lots(cum),
        end_vid,
        end_px_qty,
    }
}

/// What I1 holds equal: the state, the venue's cumulative fill, the venue id, and the price and
/// total when the terminal update states them (a terminal update that does not leaves them as
/// whichever earlier update arrived last before it).
#[derive(Clone, PartialEq, Debug)]
struct Final {
    state: OrdState,
    cum_venue: Lots,
    vid: Option<VenueOrderId>,
    px_qty: Option<(Option<Ticks>, Lots)>,
}

fn project(rec: &OrderRecord, states_px_qty: bool) -> Final {
    Final {
        state: rec.state(),
        cum_venue: rec.cum_venue(),
        vid: rec.vid().cloned(),
        px_qty: states_px_qty.then(|| (rec.px(), rec.qty())),
    }
}

fn play<'a>(
    rec: &mut OrderRecord,
    updates: impl IntoIterator<Item = &'a (OrderUpdate, Option<u64>)>,
) {
    for (ingest, (u, venue)) in updates.into_iter().enumerate() {
        let key = OrderKey {
            venue: *venue,
            ingest: ingest as u64,
        };
        rec.apply_update(u, key);
    }
}

fn end() -> impl Strategy<Value = End> {
    prop_oneof![
        Just(End::Filled),
        prop_oneof![
            Just(CancelReason::Requested),
            Just(CancelReason::PostOnly),
            Just(CancelReason::Venue)
        ]
        .prop_map(End::Canceled),
        prop_oneof![
            Just(RejectKind::PostOnlyWouldCross),
            Just(RejectKind::Margin),
            Just(RejectKind::Other)
        ]
        .prop_map(End::Rejected),
        Just(End::Expired),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    (
        any::<bool>(),
        0i64..=2,
        0u64..=1,
        any::<bool>(),
        90i64..=110,
        1i64..=20,
    )
        .prop_map(
            |(amend, cum_up, key_up, states_px_qty, new_px, new_qty)| Step {
                amend,
                cum_up,
                key_up,
                states_px_qty,
                new_px,
                new_qty,
            },
        )
}

fn history() -> impl Strategy<Value = History> {
    (
        prop::collection::vec(step(), 0..7),
        end(),
        0i64..=2,
        0u64..=1,
        any::<bool>(),
        any::<bool>(),
        prop_oneof![
            Just(VidMode::Absent),
            Just(VidMode::Kept),
            Just(VidMode::Replaced)
        ],
    )
        .prop_map(
            |(steps, end, end_cum_up, end_key_up, end_states_px_qty, venue_keys, vids)| History {
                steps,
                end,
                end_cum_up,
                end_key_up,
                end_states_px_qty,
                venue_keys,
                vids,
            },
        )
}

/// A history and a delivery of it: every update's index one to three times, shuffled.
fn delivered() -> impl Strategy<Value = (History, Vec<usize>)> {
    history().prop_flat_map(|h| {
        let n = h.steps.len() + 1;
        let copies = prop::collection::vec(1usize..=3, n);
        (Just(h), copies).prop_flat_map(|(h, copies)| {
            let order: Vec<usize> = copies
                .iter()
                .enumerate()
                .flat_map(|(i, c)| std::iter::repeat_n(i, *c))
                .collect();
            (Just(h), Just(order).prop_shuffle())
        })
    })
}

proptest! {
    #![proptest_config(config(4096, 0x0005_0001))]

    /// I1 for cumulative order updates of one order.
    #[test]
    fn i1_the_final_state_does_not_depend_on_arrival_order_duplicates_or_key_ties(
        (h, order) in delivered()
    ) {
        let cid = cid();
        let produced = produce(&h, cid);

        let mut in_order = OrderRecord::new(placement(cid, 100, 10));
        play(&mut in_order, &produced.updates);
        let expected = project(&in_order, h.end_states_px_qty);
        prop_assert_eq!(expected.state, produced.end);
        prop_assert_eq!(expected.cum_venue, produced.end_cum);
        prop_assert_eq!(&expected.vid, &produced.end_vid);
        if let Some((px, qty)) = produced.end_px_qty {
            prop_assert_eq!(expected.px_qty, Some((Some(px), qty)));
        }

        let mut shuffled = OrderRecord::new(placement(cid, 100, 10));
        play(&mut shuffled, order.iter().map(|i| &produced.updates[*i]));
        prop_assert_eq!(project(&shuffled, h.end_states_px_qty), expected);
    }
}

// ---- I2 ----

#[derive(Copy, Clone, Debug)]
enum St {
    Open,
    Amended(Option<usize>),
    Filled,
    Canceled,
    Rejected,
    Expired,
}

#[derive(Copy, Clone, Debug)]
enum Out {
    NotSent,
    Accepted,
    Rejected(RejectKind),
    Unknown,
}

#[derive(Clone, Debug)]
enum Op {
    Update {
        st: St,
        vid: Option<usize>,
        cum: i64,
        px_qty: Option<(i64, i64)>,
        venue: Option<u64>,
    },
    Outcome {
        op: OrderOp,
        vid: Option<usize>,
        out: Out,
    },
    AmendSent {
        px: i64,
        qty: i64,
    },
    CancelSent,
}

fn op() -> impl Strategy<Value = Op> {
    let pool = || prop::option::of(0usize..3);
    let st = prop_oneof![
        Just(St::Open),
        pool().prop_map(St::Amended),
        Just(St::Filled),
        Just(St::Canceled),
        Just(St::Rejected),
        Just(St::Expired),
    ];
    let order_op = prop_oneof![
        Just(OrderOp::Place),
        Just(OrderOp::Amend),
        Just(OrderOp::Cancel)
    ];
    let out = prop_oneof![
        Just(Out::NotSent),
        Just(Out::Accepted),
        prop_oneof![
            Just(RejectKind::PostOnlyWouldCross),
            Just(RejectKind::NotFound),
            Just(RejectKind::AlreadyTerminal(TerminalHint::Unspecified)),
            Just(RejectKind::NoChange),
        ]
        .prop_map(Out::Rejected),
        Just(Out::Unknown),
    ];
    prop_oneof![
        4 => (
            st,
            pool(),
            0i64..=12,
            prop::option::of((90i64..=110, 1i64..=12)),
            prop::option::of(0u64..4),
        )
            .prop_map(|(st, vid, cum, px_qty, venue)| Op::Update { st, vid, cum, px_qty, venue }),
        2 => (order_op, pool(), out).prop_map(|(op, vid, out)| Op::Outcome { op, vid, out }),
        1 => (90i64..=110, 1i64..=12).prop_map(|(px, qty)| Op::AmendSent { px, qty }),
        1 => Just(Op::CancelSent),
    ]
}

fn to_update(cid: fbc_core::ClientOrderId, st: St, vid: Option<usize>, cum: i64) -> OrderUpdate {
    let state = match st {
        St::Open => VenueOrderState::Open,
        St::Amended(new_vid) => VenueOrderState::Amended {
            new_vid: new_vid.map(venue_id),
        },
        St::Filled => VenueOrderState::Filled,
        St::Canceled => VenueOrderState::Canceled(CancelReason::Requested),
        St::Rejected => rejected(RejectKind::PostOnlyWouldCross),
        St::Expired => VenueOrderState::Expired,
    };
    let mut u = update(Some(cid), state, cum);
    u.vid = vid.map(venue_id);
    u
}

fn terminal_of(st: St) -> Option<TerminalKind> {
    match st {
        St::Open | St::Amended(_) => None,
        St::Filled => Some(TerminalKind::Filled),
        St::Canceled => Some(TerminalKind::Canceled(CancelReason::Requested)),
        St::Rejected => Some(TerminalKind::Rejected(RejectKind::PostOnlyWouldCross)),
        St::Expired => Some(TerminalKind::Expired),
    }
}

fn to_outcome(out: Out) -> SubmitOutcome {
    match out {
        Out::NotSent => SubmitOutcome::NotSent(NotSentReason::Disconnected),
        Out::Accepted => SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        },
        Out::Rejected(kind) => SubmitOutcome::Rejected(Reject {
            kind,
            venue_code: None,
            raw: "refused".into(),
        }),
        Out::Unknown => SubmitOutcome::Unknown,
    }
}

proptest! {
    #![proptest_config(config(4096, 0x0005_0002))]

    /// I2, with the lattice's other monotone parts: the state's rank and the venue's cumulative
    /// fill never fall.
    #[test]
    fn i2_a_terminal_order_never_leaves_terminal_and_a_terminal_update_always_applies(
        ops in prop::collection::vec(op(), 0..40)
    ) {
        let cid = cid();
        let mut rec = OrderRecord::new(placement(cid, 100, 10));
        for (ingest, op) in ops.into_iter().enumerate() {
            let before = rec.state();
            let superseded: Vec<VenueOrderId> = rec.superseded_vids().cloned().collect();
            let cum_before = rec.cum_venue();
            match op {
                Op::Update { st, vid, cum, px_qty, venue } => {
                    let mut u = to_update(cid, st, vid, cum);
                    if let Some((px, qty)) = px_qty {
                        (u.px, u.qty) = (Some(Ticks(px)), Some(lots(qty)));
                    }
                    let applied = rec.apply_update(&u, OrderKey { venue, ingest: ingest as u64 });
                    if before.is_terminal() {
                        prop_assert_eq!(applied, Applied::IgnoredLate);
                    } else if let Some(kind) = terminal_of(st) {
                        if u.vid.as_ref().is_some_and(|v| superseded.contains(v)) {
                            prop_assert_eq!(applied, Applied::IgnoredSupersededVid);
                            prop_assert_eq!(rec.state(), before);
                        } else {
                            prop_assert_eq!(applied, Applied::Advanced);
                            prop_assert_eq!(rec.state(), OrdState::Terminal(kind));
                        }
                    }
                }
                Op::Outcome { op, vid, out } => {
                    let vid = vid.map(venue_id);
                    rec.on_outcome(op, vid.as_ref(), &to_outcome(out), MonoNs(ingest as u64));
                }
                Op::AmendSent { px, qty } => {
                    rec.amend_sent(Ticks(px), lots(qty), RpcId(ingest as u64), MonoNs(0));
                }
                Op::CancelSent => {
                    rec.cancel_sent(RpcId(ingest as u64), MonoNs(0));
                }
            }
            if before.is_terminal() {
                prop_assert_eq!(rec.state(), before);
            }
            prop_assert!(rec.state().rank() >= before.rank());
            prop_assert!(rec.cum_venue() >= cum_before);
        }
    }
}
