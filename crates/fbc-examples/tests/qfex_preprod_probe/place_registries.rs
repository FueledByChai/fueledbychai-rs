//! FBC-oxjs's done line: the place stages' registries (`ledger.rs`; decision 0148 items 3 and 7,
//! FBC-5hif), built to 0151 items 1, 9 and 12. Declared by path in `tests/qfex_preprod_probe.rs`,
//! so this proof is part of the test binary decision 0148's Consequences name (0151 item 12) and
//! Cargo builds no target of its own for it. Nothing connects: each registry, its `Link` and the
//! journal it trips are driven in memory, every fill handed to the `Link` as a session hands it
//! one, and every key, id, cap and size is synthetic.
//!
//! - each place-stage registry carries `CodProbeRun::owner_supervised_places` of its one market
//!   and no `TestnetRun`, its Start arming only while the position is flat and its Flatten
//!   reaching Exit after a fill moved it, while stage one's production registries still carry no
//!   declaration;
//! - each is bound to its account and carries its caps and lease names;
//! - the lists' entries other than the flatten IOCs are decision 0148 item 7's most per account,
//!   each list carries `--flatten-attempts` flatten IOCs, and the account's one flatten budget is
//!   stated as that many (the driver's Allowance spends it, FBC-23ch);
//! - each registry's fill ceiling is its list in size steps, and a count bound at or below any
//!   list's ceiling is refused naming `--ledger-max-entries`;
//! - a fill heard live and replayed in the registry's second session through its `Link` is a
//!   duplicate, never `Untimed`, the `Link`'s ledger keeps its step's bounds and the watermark
//!   its registry opened at, and the `Link` lists every fill its ledger refuses for its time;
//! - `PlaceRun::session_ended` records the ledger's horizon, length and replay counts, and the
//!   registry's trip, given to the journal, moves its Quoting market to Cancel-only when the
//!   journal reaches its soft limit.

use super::*;

use std::cell::RefCell;
use std::rc::Rc;

use fbc_core::{
    AccountKey, Envelope, ExchNs, ExchTsKind, ExecEvent, FillEvent, InstrumentId, Lots,
    MarketLease, OrderCaps, SignedLots, VenueSymbol,
};
use fbc_journal::Marker;
use fbc_oms::{
    ArmRefusal, EntryState, ExitKind, FillLedger, Horizon, LadderConfig, LeaseKeys, Leases,
    LedgerConfig, MarketCapsConfig, PreTradeCaps, Registry, ReplayCounts,
};
use fbc_runtime::{
    ConsumerInput, ConsumerInputs, ConsumerInputsConfig, ConsumerTimers, ExecHandler, IngestClock,
    Journal,
};
use ledger::{
    FLATTEN, MayPlace, PAGED, PLACES_DECLARATION, PlaceBounds, PlaceLists, PlaceRegistry, Step,
    TAKER, THROUGH, X, Y,
};
use link::{FillRefusal, Link, LinkHandler, Note};
use tokio::sync::Notify;

/// The one market each test registry trades: the market `qfex_fill`'s fills name.
const INST: InstrumentId = InstrumentId::new(1);
/// The two named accounts' keys.
const PRIMARY: AccountKey = AccountKey::new(1);
const SUBACCOUNT: AccountKey = AccountKey::new(2);

/// QFEX's order caps, as the probe's codec declares them.
fn order_caps() -> OrderCaps {
    probe_caps()
        .exec
        .expect("the probe's codec enters orders")
        .order
}

/// A made-up venue symbol for `INST`.
fn symbol() -> VenueSymbol {
    fbc_core::dispatch_market_data(&probe_caps(), |scope| scope.venue_symbol("SYN-USD")).unwrap()
}

/// Synthetic caps on `INST` alone: three lots each way.
fn caps() -> PreTradeCaps {
    let three = Lots::new(3);
    let limits = MarketCapsConfig {
        inventory: three,
        resting: three,
    };
    PreTradeCaps::new().with_market(INST, limits).unwrap()
}

/// The lease names of the account the probe calls `account`, naming `INST`.
fn keys(account: &str) -> LeaseKeys {
    LeaseKeys::new(Qfex.id(), account, &order_caps()).with_market(INST, symbol())
}

/// The market lease on `INST` under `reg`'s names, taken in `dir`.
fn market_lease(dir: &Path, reg: &Registry) -> MarketLease {
    let keys = reg.lease_keys().expect("the registry is named");
    MarketLease::acquire(dir, keys.venue(), keys.account(), &symbol()).unwrap()
}

/// The ladder the `Link` judges resyncs under.
fn ladder() -> LadderConfig {
    LadderConfig::new(
        Duration::from_secs(5),
        Duration::ZERO,
        Duration::from_secs(60),
        2,
    )
    .unwrap()
}

/// The command line's ledger bounds, as `probe_args` accepts them against stage one alone.
fn parsed(max_entries: usize, span: Duration) -> ledger::Bounds {
    let age = span + Duration::from_secs(1);
    ledger::Bounds::new(age, max_entries, span, &ledger::stage_one()).unwrap()
}

/// Lists for a minimum size of `min` size steps and `attempts` flatten attempts.
fn lists(min: i64, attempts: u64) -> PlaceLists {
    PlaceLists::new(Lots::new(min).unwrap(), attempts).unwrap()
}

/// `step`'s registry of `account`, on `INST`, under bounds that admit every list at a minimum
/// size of one step and one flatten attempt, opened at `start`.
fn place_registry(
    step: Step,
    account: &'static str,
    acct: AccountKey,
    start: MonoNs,
) -> PlaceRegistry {
    let bounds = PlaceBounds::accept(&parsed(100, Duration::from_secs(600)), &lists(1, 1));
    let bounds = bounds.unwrap();
    PlaceRegistry::new(
        step,
        account,
        acct,
        &bounds,
        opened(start),
        INST,
        caps(),
        keys(account),
    )
}

/// `fill`, as a session hands it to its handler: arrived at `at`, the `n`th input, timed by the
/// matching engine at `exch` when the venue timed it.
fn fill_at(fill: FillEvent, n: u64, at: MonoNs, exch: Option<i64>) -> Envelope<ExecEvent> {
    Envelope {
        stamp: Stamp {
            ingest_seq: n,
            kernel_rx: None,
            recv_mono: at,
            recv_wall: WALL,
            conn: conn(0),
        },
        exch_ts: exch.map(ExchNs),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: None,
        body: ExecEvent::Fill(fill),
    }
}

/// A session's handler over `link`, as the driver builds one.
fn handler(link: &Rc<RefCell<Link>>) -> LinkHandler {
    LinkHandler {
        link: Rc::clone(link),
        wake: Box::new({
            let wake = Rc::new(Notify::new());
            move || wake.notify_waiters()
        }),
    }
}

/// What the `Link` noted of the last fill it was handed: what it saw and whether it is
/// unexplained.
fn last_fill(link: &Link) -> (String, bool) {
    let last = link.notes().iter().rev().find_map(|n| match n {
        Note::Fill {
            seen, unexplained, ..
        } => Some((seen.clone(), *unexplained)),
        _ => None,
    });
    last.expect("a fill was noted")
}

#[test]
fn place_stage_registries_carry_owner_supervised_places_of_their_market() {
    let start = MonoNs(0);
    for step in Step::ALL {
        let built = place_registry(step, "primary", PRIMARY, start);
        let reg = built.registry();
        // The one market the declaration names, and no testnet declaration (decision 0148
        // item 3; 0151 item 1 builds no pre-production place path).
        assert_eq!(reg.cod_probe(), Some(INST), "{step:?}");
        assert!(!reg.testnet_run(), "{step:?}");
        let run = built.run();
        assert_eq!((run.step, run.market), (step, INST));
        assert_eq!(run.declaration, PLACES_DECLARATION);
        assert_eq!(run.declaration, "CodProbeRun::owner_supervised_places");
        assert!(!run.registry_run().testnet_run);
        assert_eq!(run.registry_run().account, "primary");
        assert!(run.registry_run().ends.is_empty());

        // The place stages' Start, which `cod_probe` alone cannot tell from `owner_supervised`'s:
        // on the flat hand seed it arms into Quoting; once a fill moved the position it is
        // refused, while Flatten still reaches Exit (decision 0148 item 3). Under
        // `owner_supervised`, Start would arm the moved seed. Each step's registry is held to it.
        let (dir, _removed) = temp_dir("place-declared");
        let (link, _run) = built.into_link(order_caps(), ladder());
        let link = Rc::new(RefCell::new(link));
        link.borrow_mut()
            .reg()
            .seed_position(INST, SignedLots(0))
            .unwrap();
        let lease = Leases::market(market_lease(&dir, link.borrow().registry()));
        let entry = link.borrow_mut().reg().start(INST, lease).unwrap();
        let armed = (entry.armed(), entry.state());
        assert_eq!(armed, (true, EntryState::Quoting), "{step:?}");
        let fill = qfex_fill(our_cid(), T1, false);
        handler(&link).on_exec(fill_at(fill, 1, MonoNs(1), None));
        assert_eq!(link.borrow().registry().position(INST), Some(SignedLots(1)));
        let mut link = link.borrow_mut();
        assert_eq!(
            link.reg().start(INST, Leases::none()),
            Err(ArmRefusal::SeededByHand(INST)),
            "{step:?}"
        );
        let entry = link.reg().flatten(INST, Leases::none()).unwrap();
        assert_eq!(
            entry.state(),
            EntryState::Exit(ExitKind::Flatten),
            "{step:?}"
        );
    }
    let names: Vec<&str> = Step::ALL.iter().map(|s| s.name()).collect();
    assert_eq!(names, ["drop", "cross", "take", "pages"]);
}

#[test]
fn stage_one_production_registries_still_carry_none() {
    let bounds = parsed(1, Duration::from_secs(600));
    let opened = opened(MonoNs(0));
    // On production: no probe declaration and no testnet declaration (decision 0143 item 3:
    // stage one seeds and arms no market; 0148 item 3 declares the place stages' alone).
    let production =
        ledger::RegistryLedger::new("primary", PRIMARY, &bounds, opened, Environment::Production);
    assert_eq!(production.registry().cod_probe(), None);
    assert!(!production.registry().testnet_run());
    assert!(!production.run().testnet_run);
    assert_eq!(production.registry().account(), Some(PRIMARY));
    // On pre-production, as decision 0132 item 7 says: a declared testnet run, and still no
    // probe declaration.
    let preprod = ledger::RegistryLedger::new(
        "primary",
        PRIMARY,
        &bounds,
        opened,
        Environment::PreProduction,
    );
    assert_eq!(preprod.registry().cod_probe(), None);
    assert!(preprod.registry().testnet_run());
}

#[test]
fn each_place_stage_registry_is_bound_to_its_account() {
    let start = MonoNs(0);
    for (account, acct) in [("primary", PRIMARY), ("subaccount", SUBACCOUNT)] {
        for step in Step::ALL {
            let built = place_registry(step, account, acct, start);
            let reg = built.registry();
            assert_eq!(reg.account(), Some(acct), "{account} {step:?}");
            // Its caps, and the lease names of its own account on its one market.
            assert_eq!(reg.caps(), &caps());
            assert_eq!(reg.lease_keys(), Some(&keys(account)));
            let named = reg.lease_keys().unwrap();
            assert_eq!((named.venue(), named.account()), (Qfex.id(), account));
            assert_eq!(named.symbol(INST), Some(&symbol()));
            assert_eq!(named.nonce_scope(), order_caps().nonce_scope);
            assert_eq!(built.run().registry_run().account, account);
            // Handed to its Link, the registry keeps its binding: a second account is refused
            // (decision 0082).
            let (mut link, _run) = built.into_link(order_caps(), ladder());
            assert_eq!(link.registry().account(), Some(acct));
            let other = AccountKey::new(acct.get() + 10);
            assert!(link.reg().bind_account(other).is_err());
            assert!(link.reg().bind_account(acct).is_ok());
        }
    }
}

#[test]
fn the_lists_hold_decision_0148s_per_account_maximum_with_one_flatten_budget() {
    for attempts in 1..=ledger::MAX_FLATTEN_ATTEMPTS {
        let min = 2;
        let lists = lists(min, attempts);
        let mut own: Vec<(&str, u64)> = Vec::new();
        let mut flatten_entries = 0;
        for step in Step::ALL {
            let list = lists.list(step);
            // Every place is one minimum size (decision 0148 item 4).
            assert!(list.iter().all(|p| p.largest_steps == 2), "{list:?}");
            // Each list ends with the flatten IOCs: --flatten-attempts of them, since an
            // unexpected fill may need a flatten from any registry (0151 item 9).
            let (last, rest) = list.split_last().unwrap();
            let flatten = MayPlace {
                what: FLATTEN,
                orders: attempts,
                largest_steps: 2,
            };
            assert_eq!(last, &flatten, "{step:?}");
            assert!(rest.iter().all(|p| p.what != FLATTEN));
            flatten_entries += last.orders;
            own.extend(rest.iter().map(|p| (p.what, p.orders)));
        }
        // The entries other than the flatten IOCs, across the account's four registries, are
        // decision 0148 item 7's most per account: X and Y, one post-only through the touch,
        // one taker IOC, and three stage-three post-onlys.
        assert_eq!(own, [(X, 1), (Y, 1), (THROUGH, 1), (TAKER, 1), (PAGED, 3)]);
        let own_total: u64 = own.iter().map(|(_, n)| n).sum();
        assert_eq!(own_total, 7);
        // The four lists repeat the flatten IOCs, and the account's one budget is stated as
        // --flatten-attempts, once and not once per registry. This only states it: the driver's
        // Allowance spends it across the account's registries (FBC-23ch), so the account's most
        // in a run is item 7's orders plus that budget once.
        assert_eq!(flatten_entries, 4 * attempts);
        assert_eq!(lists.flatten_budget(), attempts);
    }

    // --flatten-attempts outside 1 to 3 is refused naming the flag, never the value; a minimum
    // size of zero steps, which no order is placed at, is refused.
    for attempts in [0, 4, 4_242] {
        let err = PlaceLists::new(Lots::new(1).unwrap(), attempts).unwrap_err();
        assert_eq!(err, "--flatten-attempts: not from 1 to 3 (not shown)");
    }
    let err = PlaceLists::new(Lots::ZERO, 1).unwrap_err();
    assert!(err.contains("minimum size is zero"), "{err}");
}

#[test]
fn the_ceiling_of_each_place_stage_registry_is_its_list_in_steps() {
    let span = Duration::from_secs(600);
    // A minimum size of five steps and two flatten attempts: each order counted at five steps.
    let lists = lists(5, 2);
    let ceilings = [
        (Step::Drop, (1 + 1 + 2) * 5),
        (Step::Cross, (1 + 2) * 5),
        (Step::Take, (1 + 2) * 5),
        (Step::Pages, (3 + 2) * 5),
    ];
    let accepted = PlaceBounds::accept(&parsed(26, span), &lists).unwrap();
    for (step, ceiling) in ceilings {
        let bounds = accepted.of(step);
        assert_eq!(bounds.ceiling(), ceiling, "{step:?}");
        assert_eq!(ledger::fill_ceiling(&lists.list(step)), Ok(ceiling));
        assert_eq!(bounds.places(), lists.list(step).as_slice());
        // The command line's other bounds, as given.
        assert_eq!(bounds.config(), parsed(26, span).config());
        // A registry takes its own step's bounds.
        let opened = opened(MonoNs(0));
        let reg = PlaceRegistry::new(
            step,
            "primary",
            PRIMARY,
            &accepted,
            opened,
            INST,
            caps(),
            keys("primary"),
        );
        assert_eq!(&reg.run().registry_run().bounds, bounds);
    }

    // A count bound at a ceiling, or below one, is refused naming --ledger-max-entries and the
    // registry whose ceiling it does not clear (the first in run order), never the value typed:
    // at stage three's ceiling, and above the drop test's but below stage three's.
    for (count, refused) in [(25, "pages"), (21, "pages"), (20, "drop"), (1, "drop")] {
        let err = PlaceBounds::accept(&parsed(count, span), &lists).unwrap_err();
        assert!(
            err.starts_with(&format!(
                "the {refused} registry: --ledger-max-entries must be above"
            )),
            "{err}"
        );
        assert!(
            err.contains("(decision 0132 item 5; the value is not shown)"),
            "{err}"
        );
        if count == 21 {
            assert!(!err.contains("21"), "{err}");
        }
    }
    // A ceiling that overflows is refused too.
    let huge = PlaceLists::new(Lots::new(i64::MAX).unwrap(), 3).unwrap();
    let err = PlaceBounds::accept(&parsed(26, span), &huge).unwrap_err();
    assert!(
        err.starts_with("the drop registry: the fill ceiling overflows"),
        "{err}"
    );
}

#[tokio::test]
async fn a_replayed_fill_in_a_second_session_through_the_link_is_a_duplicate_not_untimed() {
    let span = Duration::from_secs(600);
    let start = MonoNs(5_000_000_000);
    let cid = our_cid();
    let live = qfex_fill(cid, T1, false);
    let replay = qfex_fill(cid, T1, true);
    assert_eq!(live.key(), replay.key());

    // The drop test's registry serves both its sessions through one Link and one ledger.
    let built = place_registry(Step::Drop, "primary", PRIMARY, start);
    let (link, mut run) = built.into_link(order_caps(), ladder());
    assert!(run.deadline() >= tokio::time::Instant::now() + span - Duration::from_secs(1));
    let link = Rc::new(RefCell::new(link));
    let mut handler = handler(&link);
    // Session one: the fill heard live, with no venue time (QFEX's until FBC-jdj7), is applied.
    handler.on_exec(fill_at(live, 1, start, None));
    assert_eq!(
        last_fill(&link.borrow()).0,
        format!("{:?}", fbc_oms::FillRouted::OursUntracked(cid))
    );
    run.session_ended(
        &link.borrow(),
        "drop session one",
        start + Duration::from_secs(100),
    );
    // Session two, at the span bound: its resync reads the fill again, as a replay with no
    // venue time. The ledger the Link kept still holds it, so it is a duplicate, never refused
    // Untimed, and the Link lists no refusal.
    handler.on_exec(fill_at(replay, 2, start + span, None));
    assert_eq!(
        last_fill(&link.borrow()),
        ("not applied: Duplicate".to_owned(), false)
    );
    assert!(link.borrow().fill_refusals().is_empty());
    run.session_ended(&link.borrow(), "drop session two", start + span);

    // session_ended read the Link's ledger: its horizon, the fills it held and its replays.
    let ends: Vec<(&str, Duration, Horizon, usize)> = (run.registry_run().ends.iter())
        .map(|e| (e.session, e.after, e.horizon, e.held))
        .collect();
    assert_eq!(
        ends,
        [
            (
                "drop session one",
                Duration::from_secs(100),
                Horizon::Full,
                1
            ),
            ("drop session two", span, Horizon::Full, 1),
        ]
    );
    assert_eq!(run.registry_run().ends[0].replays, ReplayCounts::default());
    let duplicate = ReplayCounts {
        duplicate: 1,
        ..ReplayCounts::default()
    };
    assert_eq!(run.registry_run().ends[1].replays, duplicate);
    assert_eq!(link.borrow().ledger().replays(), duplicate);
    assert!(run.registry_run().held());
}

#[tokio::test]
async fn a_place_registrys_link_keeps_its_steps_bounds_and_its_opened_watermark() {
    let span = Duration::from_secs(600);
    let start = MonoNs(5_000_000_000);
    let cid = our_cid();
    // A count bound of five, the least above stage three's ceiling at one size step and one
    // flatten attempt (four), and an age bound one second past the span bound (`parsed`).
    let bounds = PlaceBounds::accept(&parsed(5, span), &lists(1, 1)).unwrap();
    let opened = opened(start);
    let built = PlaceRegistry::new(
        Step::Pages,
        "primary",
        PRIMARY,
        &bounds,
        opened,
        INST,
        caps(),
        keys("primary"),
    );
    let (link, _run) = built.into_link(order_caps(), ladder());
    let link = Rc::new(RefCell::new(link));
    let mut on = handler(&link);

    // The watermark is the wall time the registry opened at: a replay timed at it is refused
    // BeforeWatermark, which the starting position holds.
    assert_eq!(link.borrow().ledger().watermark(), opened.wall);
    on.on_exec(fill_at(
        qfex_fill(cid, T1, true),
        1,
        start,
        Some(opened.wall.0),
    ));
    assert_eq!(
        last_fill(&link.borrow()),
        ("not applied: BeforeWatermark".to_owned(), false)
    );

    // The count bound is the step's: five fills held, the sixth forgets the first, whose
    // matching-engine time is then the horizon.
    let trades = [T1, T2, T3, "f4", "f5", "f6"];
    for (n, trade) in (2..).zip(trades) {
        let exch = 1_000 * i64::try_from(n).unwrap();
        on.on_exec(fill_at(qfex_fill(cid, trade, false), n, start, Some(exch)));
    }
    assert_eq!(link.borrow().ledger().len(), 5);
    assert_eq!(link.borrow().ledger().horizon(), Horizon::At(ExchNs(2_000)));

    // The age bound is the step's: at the span bound nothing has aged out (the count bound
    // forgets the next oldest), and a second later every fill applied at the start has.
    on.on_exec(fill_at(
        qfex_fill(cid, "f7", false),
        8,
        start + span,
        Some(8_000),
    ));
    assert_eq!(link.borrow().ledger().len(), 5);
    assert_eq!(link.borrow().ledger().horizon(), Horizon::At(ExchNs(3_000)));
    let past = start + span + Duration::from_secs(1);
    on.on_exec(fill_at(qfex_fill(cid, "f8", false), 9, past, Some(9_000)));
    assert_eq!(link.borrow().ledger().len(), 2);
    assert_eq!(link.borrow().ledger().horizon(), Horizon::At(ExchNs(7_000)));
    assert!(link.borrow().fill_refusals().is_empty());
}

#[tokio::test]
async fn untimed_and_beyond_horizon_refusals_are_listed_in_fill_refusals() {
    let span = Duration::from_secs(600);
    let start = MonoNs(5_000_000_000);
    let cid = our_cid();
    let link_over = |config: LedgerConfig| {
        let ledger = FillLedger::new(config, WallNs(1_000)).unwrap();
        Rc::new(RefCell::new(Link::new(
            Registry::new(),
            ledger,
            order_caps(),
            ladder(),
        )))
    };

    // With the age bound equal to the span bound, which the program refuses, the replay at the
    // span bound finds the fill forgotten: refused Untimed, and listed.
    let refused = ledger::Bounds::new(span, 1, span, &ledger::stage_one()).unwrap_err();
    assert!(refused.contains("--ledger-max-age must be longer than --registry-span"));
    let link = link_over(LedgerConfig {
        max_age: span,
        max_entries: 1,
    });
    let mut on = handler(&link);
    on.on_exec(fill_at(qfex_fill(cid, T1, false), 1, start, None));
    assert!(link.borrow().fill_refusals().is_empty());
    on.on_exec(fill_at(qfex_fill(cid, T1, true), 2, start + span, None));
    let untimed = FillRefusal {
        admission: "Untimed",
        at: start + span,
    };
    assert_eq!(link.borrow().fill_refusals(), [untimed]);
    assert_eq!(
        last_fill(&link.borrow()),
        ("not applied: Untimed".to_owned(), true)
    );
    assert_eq!(link.borrow().ledger().horizon(), Horizon::Lost);

    // A count bound of one: the second fill forgets the first, whose matching-engine time then
    // bounds the horizon; a replay of it at that time is refused BeyondHorizon, and listed. A
    // replay before the watermark, which the starting position holds, is not.
    let link = link_over(LedgerConfig {
        max_age: Duration::from_secs(10),
        max_entries: 1,
    });
    let mut on = handler(&link);
    on.on_exec(fill_at(qfex_fill(cid, T1, false), 1, start, Some(2_000)));
    on.on_exec(fill_at(qfex_fill(cid, T2, false), 2, start, Some(3_000)));
    on.on_exec(fill_at(qfex_fill(cid, T1, true), 3, start, Some(2_000)));
    assert_eq!(
        last_fill(&link.borrow()),
        ("not applied: BeyondHorizon".to_owned(), true)
    );
    on.on_exec(fill_at(qfex_fill(cid, T3, true), 4, start, Some(500)));
    assert_eq!(
        last_fill(&link.borrow()),
        ("not applied: BeforeWatermark".to_owned(), false)
    );
    let beyond = FillRefusal {
        admission: "BeyondHorizon",
        at: start,
    };
    assert_eq!(link.borrow().fill_refusals(), [beyond]);
    assert_eq!(link.borrow().ledger().horizon(), Horizon::At(ExchNs(2_000)));
    let counts = link.borrow().ledger().replays();
    assert_eq!((counts.beyond_horizon, counts.before_watermark), (1, 1));
}

/// A journal sink with no room for anything: every record dropped and counted, each marker of a
/// move to Cancel-only counted.
#[derive(Default)]
struct Full {
    moves: usize,
}

impl fbc_journal::JournalSink for Full {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        if matches!(record, Record::Marker(Marker::CancelOnly)) {
            self.moves += 1;
        }
        Recorded::DroppedCounted
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }
}

#[tokio::test]
async fn a_place_registry_hands_its_trip_to_the_journal() {
    let (dir, _removed) = temp_dir("place-trip");
    let start = MonoNs(0);
    let sink = Rc::new(RefCell::new(Full::default()));
    let journal = Journal::new(Rc::clone(&sink));
    // Stage three's registry of the subaccount is given to the journal; the take registry of
    // the primary account is not.
    let given = place_registry(Step::Pages, "subaccount", SUBACCOUNT, start);
    given.trip_on(&journal);
    let other = place_registry(Step::Take, "primary", PRIMARY, start);
    let quoting = |built: PlaceRegistry| {
        let (mut link, run) = built.into_link(order_caps(), ladder());
        link.reg().seed_position(INST, SignedLots(0)).unwrap();
        let lease = Leases::market(market_lease(&dir, link.registry()));
        let entry = link.reg().start(INST, lease).unwrap();
        assert_eq!(entry.state(), EntryState::Quoting);
        (link, run)
    };
    let (given, mut run) = quoting(given);
    let (other, _) = quoting(other);

    // session_ended at a session's end with no fill: the horizon full, no fill held, no
    // replay.
    run.session_ended(&given, "pages", start + Duration::from_secs(1));
    let end = &run.registry_run().ends[0];
    assert_eq!((end.session, end.after), ("pages", Duration::from_secs(1)));
    assert_eq!((end.horizon, end.held), (Horizon::Full, 0));
    assert_eq!(end.replays, ReplayCounts::default());
    assert_eq!(run.registry_run().account, "subaccount");

    // A record of the runtime's the full sink drops reaches the journal's soft limit: here a
    // consumer input the shard journals (decision 0103). The given registry's market moves to
    // Cancel-only, still armed, and the move is journaled (decision 0139); the other's stays
    // Quoting.
    let config = ConsumerInputsConfig {
        clock: IngestClock::new(),
        capacity: 1,
        timers: ConsumerTimers::new(),
    };
    let (mut inputs, sender) = ConsumerInputs::new(config, |_: ConsumerInput| {}).unwrap();
    inputs.set_journal(journal);
    sender.push(TrafficClass::Normal, 1, vec![0]).unwrap();
    drop(sender);
    inputs.run().await;
    assert_eq!(inputs.counters().pushed, 1);
    assert_eq!(given.registry().cancel_only_trip().trips(), 1);
    let entry = given.registry().entry(INST);
    assert_eq!(
        (entry.armed(), entry.state()),
        (true, EntryState::CancelOnly)
    );
    assert_eq!(sink.borrow().moves, 1);
    assert_eq!(other.registry().cancel_only_trip().trips(), 0);
    assert_eq!(other.registry().entry(INST).state(), EntryState::Quoting);
}

#[test]
fn a_place_registrys_line_in_decision_0132s_list_gives_its_ledger_as_words_and_counts() {
    // FBC-4plj: each registry's bounds and, at each of its sessions' ends, its horizon as a word
    // and its replay counts, never a time or a value.
    let built = place_registry(Step::Pages, "primary", PRIMARY, MonoNs(0));
    assert_eq!(
        built.run().said(),
        "the pages registry: its bounds held yes; no session of it ended"
    );
    let (link, mut run) = built.into_link(order_caps(), ladder());
    run.session_ended(&link, "the stage-three session", MonoNs(5_000_000));
    assert_eq!(
        run.said(),
        "the pages registry: its bounds held yes; after the stage-three session: horizon full, 0 \
         fills held, replays applied 0, duplicate 0, before the watermark 0, beyond the horizon \
         0, untimed 0"
    );
    use fbc_oms::Horizon;
    assert_eq!(ledger::horizon_word(Horizon::Full), "full");
    assert_eq!(ledger::horizon_word(Horizon::At(ExchNs(7))), "moved");
    assert_eq!(ledger::horizon_word(Horizon::Lost), "lost");
}
