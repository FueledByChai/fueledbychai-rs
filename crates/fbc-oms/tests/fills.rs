//! Decision 0005's I1 with fills, and I3, property-tested (decision 0037).
//!
//! I1 with fills: whenever a terminal event is present, the final state does not depend on the
//! order the cumulative order updates and the fills of the same executions arrive in, on
//! duplicates, or on ties in the updates' ordering keys. Each case generates one order's
//! executions as a venue reports them (a fill per execution, named by fill id or by venue
//! order id and cumulative quantity; cumulative order updates whose ordering keys never fall
//! and often tie; one terminal update), delivers every event one to three times in a shuffled
//! order through the fill ledger and the registry, and compares the result with the one the
//! history in order leaves. At every step the order's filled quantity is the larger of the
//! venue's cumulative count and the deduplicated fill sum, never their sum, and the inventory
//! is that fill sum.
//!
//! I3: inventory moves only through fills the ledger deduplicated; a replayed fill is applied
//! only when absent and newer than the session-start watermark; and a fill the ledger forgot
//! (by age or count) and that is then replayed never moves the inventory a second time. Each
//! case generates distinct fills with venue times around the watermark, a ledger that holds
//! one to four fills for a short age, and a shuffled delivery in which each fill's first
//! arrival may be live and every later one is a replay.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::{cid, fill, fill_id, lots, placement, rejected, update};
use fbc_core::{
    AckLevel, CancelReason, ClientOrderId, ExchNs, ExchTsKind, FillEvent, FillIdent, FillKey,
    InstrumentId, ItemRef, Lots, MonoNs, OrderUpdate, RejectKind, Side, SignedLots, SubmitOutcome,
    VenueOrderId, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, FillLedger, FillRouted, FillTime, Horizon, LedgerConfig, OrdState, OrderKey,
    OrderOp, Registry, TerminalKind,
};
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

const INST: InstrumentId = InstrumentId::new(1);

fn vid1() -> VenueOrderId {
    common::vid("v1")
}

fn signed(side: Side, qty: Lots) -> SignedLots {
    SignedLots::of(side, qty)
}

// ---- I1 with fills ----

#[derive(Copy, Clone, Debug)]
enum End {
    Filled,
    Canceled,
    Expired,
    Rejected,
}

/// How the venue names a fill.
#[derive(Copy, Clone, Debug)]
enum Naming {
    /// By its fill id, with the order's venue id and cumulative quantity when `vid` and `cum`.
    FillId { vid: bool, cum: bool },
    /// By the order's venue id and its cumulative quantity after the fill.
    Derived,
}

#[derive(Clone, Debug)]
struct Exec {
    qty: i64,
    update_after: bool,
    key_up: u64,
}

#[derive(Clone, Debug)]
struct History {
    execs: Vec<Exec>,
    end: End,
    extra: i64,
    end_key_up: u64,
    naming: Naming,
    updates_vid: bool,
    venue_keys: bool,
    accepted: bool,
    side: Side,
}

#[derive(Clone, Debug)]
enum Ev {
    Update(OrderUpdate, Option<u64>),
    Fill(FillEvent),
    Accepted,
}

struct Produced {
    events: Vec<Ev>,
    qty: i64,
    executed: i64,
    end: OrdState,
}

fn produce(h: &History, cid: ClientOrderId) -> Produced {
    let mut execs = h.execs.clone();
    match h.end {
        End::Rejected => execs.clear(),
        End::Filled if execs.is_empty() => execs.push(Exec {
            qty: 1,
            update_after: false,
            key_up: 0,
        }),
        End::Filled | End::Canceled | End::Expired => {}
    }
    let executed: i64 = execs.iter().map(|e| e.qty).sum();
    let qty = match h.end {
        End::Filled => executed,
        End::Canceled | End::Expired | End::Rejected => executed + h.extra,
    };
    let mut events = Vec::new();
    if h.accepted {
        events.push(Ev::Accepted);
    }
    let named = |u: &mut OrderUpdate| {
        u.side = h.side;
        u.vid = h.updates_vid.then(vid1);
    };
    let (mut cum, mut key) = (0i64, 0u64);
    for (i, e) in execs.iter().enumerate() {
        cum += e.qty;
        let ident = match h.naming {
            Naming::FillId { vid, cum: with_cum } => FillIdent::Venue {
                fill: fill_id(&format!("f{i}")),
                vid: vid.then(vid1),
                cum_after: with_cum.then(|| lots(cum)),
            },
            Naming::Derived => FillIdent::Derived {
                vid: vid1(),
                cum_after: lots(cum),
            },
        };
        events.push(Ev::Fill(fill(Some(cid), ident, h.side, e.qty, false)));
        if e.update_after {
            key += e.key_up;
            let mut u = update(Some(cid), VenueOrderState::Open, cum);
            named(&mut u);
            events.push(Ev::Update(u, h.venue_keys.then_some(key)));
        }
    }
    key += h.end_key_up;
    let (state, end) = match h.end {
        End::Filled => (VenueOrderState::Filled, TerminalKind::Filled),
        End::Canceled => (
            VenueOrderState::Canceled(CancelReason::Requested),
            TerminalKind::Canceled(CancelReason::Requested),
        ),
        End::Expired => (VenueOrderState::Expired, TerminalKind::Expired),
        End::Rejected => (
            rejected(RejectKind::Margin),
            TerminalKind::Rejected(RejectKind::Margin),
        ),
    };
    let mut u = update(Some(cid), state, executed);
    named(&mut u);
    events.push(Ev::Update(u, h.venue_keys.then_some(key)));
    Produced {
        events,
        qty,
        executed,
        end: OrdState::Terminal(end),
    }
}

/// What I1 holds equal. The venue's own cumulative count is not compared: an order the fills
/// alone completed ignores its later Filled update (I2), so that count depends on arrival, but
/// the filled quantity, the larger of the two counters, does not. The venue id is compared
/// when the terminal update names it, unless the order ends Filled, for the same reason (a
/// terminal order learns nothing, so an id named only by an event after the end is not kept).
#[derive(Clone, PartialEq, Debug)]
struct Final {
    state: OrdState,
    filled: Lots,
    cum_fills: Lots,
    inventory: SignedLots,
    vid: Option<Option<VenueOrderId>>,
}

/// Plays `events` through a fresh ledger and registry, checking I3's counters at every step.
fn play<'a>(
    h: &History,
    cid: ClientOrderId,
    qty: i64,
    events: impl IntoIterator<Item = &'a Ev>,
) -> Result<Final, TestCaseError> {
    let mut ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1024,
        },
        WallNs(0),
    )
    .unwrap();
    let mut reg = Registry::new();
    let mut p = placement(cid, 100, qty);
    p.side = h.side;
    reg.insert(p).unwrap();
    for (ingest, ev) in events.into_iter().enumerate() {
        let now = MonoNs(ingest as u64);
        match ev {
            Ev::Update(u, venue) => {
                let key = OrderKey {
                    venue: *venue,
                    ingest: ingest as u64,
                };
                reg.apply_update(u, key);
            }
            Ev::Fill(f) => {
                if let Admission::Apply(accepted) = ledger.admit(f, None, now) {
                    let routed = reg.apply_fill(accepted).unwrap();
                    prop_assert!(matches!(routed, FillRouted::Ours(c, _) if c == cid));
                }
            }
            Ev::Accepted => {
                let item = ItemRef {
                    idx: 0,
                    cid: Some(cid),
                    vid: Some(vid1()),
                };
                let ack = SubmitOutcome::Accepted {
                    ack: AckLevel::Final,
                };
                reg.on_outcome(cid, OrderOp::Place, &item, &ack, now)
                    .unwrap();
            }
        }
        let rec = reg.get(cid).unwrap();
        prop_assert_eq!(rec.filled(), rec.cum_venue().max(rec.cum_fills()));
        prop_assert_eq!(reg.inventory(INST), signed(h.side, rec.cum_fills()));
    }
    let rec = reg.get(cid).unwrap();
    let ends_filled = rec.state() == OrdState::Terminal(TerminalKind::Filled);
    Ok(Final {
        state: rec.state(),
        filled: rec.filled(),
        cum_fills: rec.cum_fills(),
        inventory: reg.inventory(INST),
        vid: (h.updates_vid && !ends_filled).then(|| rec.vid().cloned()),
    })
}

fn exec() -> impl Strategy<Value = Exec> {
    (1i64..=3, any::<bool>(), 0u64..=1).prop_map(|(qty, update_after, key_up)| Exec {
        qty,
        update_after,
        key_up,
    })
}

fn history() -> impl Strategy<Value = History> {
    let end = prop_oneof![
        Just(End::Filled),
        Just(End::Canceled),
        Just(End::Expired),
        Just(End::Rejected)
    ];
    let naming = prop_oneof![
        (any::<bool>(), any::<bool>()).prop_map(|(vid, cum)| Naming::FillId { vid, cum }),
        Just(Naming::Derived),
    ];
    let side = prop_oneof![Just(Side::Buy), Just(Side::Sell)];
    (
        prop::collection::vec(exec(), 0..6),
        end,
        1i64..=3,
        0u64..=1,
        naming,
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        side,
    )
        .prop_map(
            |(execs, end, extra, end_key_up, naming, updates_vid, venue_keys, accepted, side)| {
                History {
                    execs,
                    end,
                    extra,
                    end_key_up,
                    naming,
                    updates_vid,
                    venue_keys,
                    accepted,
                    side,
                }
            },
        )
}

/// A history and a delivery of it: every event's index one to three times, shuffled.
fn delivered() -> impl Strategy<Value = (History, Vec<usize>)> {
    history().prop_flat_map(|h| {
        let n = produce(&h, cid()).events.len();
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
    #![proptest_config(config(4096, 0x0005_0003))]

    /// I1 with fills, and I3's two counters: inventory only through deduplicated fills, the
    /// filled quantity their maximum, never their sum.
    #[test]
    fn i1_with_fills_the_final_state_does_not_depend_on_arrival_order_or_duplicates(
        (h, order) in delivered()
    ) {
        let cid = cid();
        let produced = produce(&h, cid);
        let executed = lots(produced.executed);

        let expected = play(&h, cid, produced.qty, &produced.events)?;
        prop_assert_eq!(expected.state, produced.end);
        prop_assert_eq!(expected.filled, executed);
        prop_assert_eq!(expected.cum_fills, executed);
        prop_assert_eq!(expected.inventory, signed(h.side, executed));
        if let Some(vid) = &expected.vid {
            prop_assert_eq!(vid, &Some(vid1()));
        }

        let shuffled = play(
            &h,
            cid,
            produced.qty,
            order.iter().map(|i| &produced.events[*i]),
        )?;
        prop_assert_eq!(shuffled, expected);
    }
}

// ---- I3: replays, the watermark and the retention horizon ----

const WATERMARK: i64 = 1_000;

/// How the venue timed one fill.
#[derive(Copy, Clone, Debug)]
enum Timed {
    Engine,
    Publish,
    Unknown,
    Untimed,
}

#[derive(Clone, Debug)]
struct Spec {
    qty: i64,
    side: Side,
    exch: i64,
    timed: Timed,
    live: bool,
    replays: usize,
}

#[derive(Clone, Debug)]
struct Session {
    specs: Vec<Spec>,
    max_entries: usize,
    max_age: u64,
    /// Fill indices in arrival order, each with how far the clock moved before it and the
    /// alignment error on that arrival's wall-clock time.
    arrivals: Vec<(usize, u64, i64)>,
}

fn spec() -> impl Strategy<Value = Spec> {
    let timed = prop_oneof![
        6 => Just(Timed::Engine),
        1 => Just(Timed::Publish),
        1 => Just(Timed::Unknown),
        1 => Just(Timed::Untimed),
    ];
    let side = prop_oneof![Just(Side::Buy), Just(Side::Sell)];
    (
        1i64..=5,
        side,
        960i64..=1_060,
        timed,
        any::<bool>(),
        0usize..=3,
    )
        .prop_map(|(qty, side, exch, timed, live, replays)| Spec {
            qty,
            side,
            exch,
            timed,
            live,
            replays,
        })
}

fn session() -> impl Strategy<Value = Session> {
    (prop::collection::vec(spec(), 1..10), 1usize..=4, 1u64..=60).prop_flat_map(
        |(specs, max_entries, max_age)| {
            let order: Vec<usize> = specs
                .iter()
                .enumerate()
                .flat_map(|(i, s)| std::iter::repeat_n(i, usize::from(s.live) + s.replays))
                .collect();
            let n = order.len();
            (
                Just(specs),
                Just(max_entries),
                Just(max_age),
                Just(order).prop_shuffle(),
                prop::collection::vec((0u64..=20, -3i64..=3), n),
            )
                .prop_map(|(specs, max_entries, max_age, order, steps)| Session {
                    specs,
                    max_entries,
                    max_age,
                    arrivals: order
                        .into_iter()
                        .zip(steps)
                        .map(|(i, (dt, skew))| (i, dt, skew))
                        .collect(),
                })
        },
    )
}

fn time_of(s: &Spec, skew: i64) -> Option<FillTime> {
    let kind = match s.timed {
        Timed::Engine => ExchTsKind::MatchingEngine,
        Timed::Publish => ExchTsKind::Publish,
        Timed::Unknown => ExchTsKind::Unknown,
        Timed::Untimed => return None,
    };
    Some(FillTime {
        exch: ExchNs(s.exch),
        kind,
        aligned: WallNs(s.exch + skew),
    })
}

proptest! {
    #![proptest_config(config(4096, 0x0005_0004))]

    /// I3: inventory only through deduplicated fills; replays applied only when absent and
    /// newer than the watermark; a forgotten fill replayed never moves inventory twice.
    #[test]
    fn i3_a_replayed_fill_moves_inventory_only_when_absent_newer_and_vouched_for(
        s in session()
    ) {
        // An order per side, each fill on the one of its side.
        let (buy, sell) = (cid(), cid());
        let mut ledger = FillLedger::new(
            LedgerConfig {
                max_age: Duration::from_nanos(s.max_age),
                max_entries: s.max_entries,
            },
            WallNs(WATERMARK),
        )
        .unwrap();
        let mut reg = Registry::new();
        reg.insert(placement(buy, 100, 1_000_000)).unwrap();
        let mut selling = placement(sell, 100, 1_000_000);
        selling.side = Side::Sell;
        reg.insert(selling).unwrap();

        let mut applied: HashMap<FillKey, u32> = HashMap::new();
        let mut expected = SignedLots(0);
        let mut seen = vec![false; s.specs.len()];
        let mut now = 0u64;
        let mut replayed = 0u64;
        for &(i, dt, skew) in &s.arrivals {
            now += dt;
            let spec = &s.specs[i];
            let replay = seen[i] || !spec.live;
            seen[i] = true;
            replayed += u64::from(replay);
            let cid = if spec.side == Side::Buy { buy } else { sell };
            let f = fill(Some(cid), common::ident(&format!("f{i}")), spec.side, spec.qty, replay);
            let time = time_of(spec, skew);
            let key = f.key();
            let before = applied.get(&key).copied().unwrap_or(0);
            let engine_time = time.filter(|t| t.kind == ExchTsKind::MatchingEngine);
            let newer = engine_time.is_some_and(|t| t.aligned > WallNs(WATERMARK));
            let vouched_by = |horizon: Horizon| match horizon {
                Horizon::Full => true,
                Horizon::At(h) => engine_time.is_some_and(|t| t.exch > h),
                Horizon::Lost => false,
            };
            // The horizon only moves forward; admitting may move it (what aged out is forgotten
            // first), so an applied replay was vouched for by the horizon before it too.
            let vouched_before = vouched_by(ledger.horizon());
            let refused = match ledger.admit(&f, time, MonoNs(now)) {
                Admission::Apply(accepted) => {
                    prop_assert_eq!(before, 0, "a fill moved inventory twice");
                    if replay {
                        prop_assert!(newer, "a replay at or before the watermark was applied");
                        prop_assert!(
                            vouched_before,
                            "a replay the ledger could not vouch for was applied"
                        );
                    }
                    let routed = reg.apply_fill(accepted).unwrap();
                    prop_assert!(matches!(routed, FillRouted::Ours(c, _) if c == cid));
                    applied.insert(key, before + 1);
                    expected = expected.checked_add(signed(spec.side, lots(spec.qty))).unwrap();
                    None
                }
                other => Some(format!("{other:?}")),
            };
            if let Some(refused) = refused {
                // A fill's first, live arrival: never seen, so always applied.
                prop_assert!(replay, "a live fill was refused: {}", refused);
                // A replay absent, newer than the watermark and vouched for by the horizon it
                // was judged under (the one after admitting) is applied.
                prop_assert!(
                    !(before == 0 && newer && vouched_by(ledger.horizon())),
                    "a replay the ledger should apply was refused: {}",
                    refused
                );
            }
            prop_assert_eq!(reg.inventory(INST), expected);
            prop_assert!(ledger.len() <= s.max_entries);
        }
        let counts = ledger.replays();
        prop_assert_eq!(
            counts.applied + counts.duplicate + counts.before_watermark
                + counts.beyond_horizon + counts.untimed,
            replayed
        );
    }
}
