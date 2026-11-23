//! A place, a batch or an amend authorized outside Exit is refused at submit once its market's
//! position moved since its authorization (FBC-zux3, decision 0153; the owner's answer A of
//! 2026-11-17, "Refuse it, replan"). 0082 judges such a command against the caps again as it is
//! authorized, so its authorization watches the market's position revision from then on: a fill
//! the side's counted orders did not already hold (an own fill of an order the registry does
//! not hold, a venue-initiated fill, a late fill on an order the registry already ended) that
//! takes the position to the inventory cap before its encode refuses it
//! ([`StaleAuthorization::PositionMoved`]), with nothing written, and so does any other move:
//! the planner builds it again on its next pass.
//!
//! The watch is taken at authorization, not at build: a command built before a fill and
//! authorized after it was judged against the moved position by 0082, and passes at submit. A
//! command built in Exit keeps the watch its build took (0066), so a fill between its build and
//! its authorization still refuses it. A fill that moves no inventory of the market (another
//! namespace's, one a seed already held, or another market's) leaves an authorization passing,
//! and a cancel always passes (0013).
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the resting cap is set past any order (`WIDE`).

#[path = "common/arm.rs"]
mod arm;
mod common;
#[path = "common/venue.rs"]
mod venue;

use std::time::Duration;

use arm::{TEST_ACCT as ACCT, start};
use common::{
    canceled, cid, fill, ident, lots, no_fee, order_caps, placement, update, venue_fill, vid,
};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Bps, Channel, CidMatch, CidMint,
    ClientOrderId, EncodeCtx, EncodeReceipt, FillEvent, FillOrigin, InstrumentId, ItemRef, MonoNs,
    Namespace, NamespaceLease, NewOrder, NonceBlock, NotSentReason, OrderCaps, OrderKind,
    PathStamps, PxExact, RefKind, RpcId, Side, SignedLots, SubmitHandle, SubmitOutcome, TagSet,
    Ticks, Tif, VenueCommand, VenueFillKind, WallNs,
};
use fbc_oms::{
    Admission, Authorization, CancelChoice, CapRefusal, ControlCommand, DesiredBook, DesiredQuote,
    ExecutionPlanner, FillLedger, FillRouted, LedgerConfig, MarketCapsConfig, OmsError, OrdState,
    OrderGateway, OrderKey, OrderOp, PermittedCommand, PlannerConfig, PreTradeCaps, ReduceFallback,
    Registry, StaleAuthorization, TerminalKind, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order of these tests.
const WIDE: i64 = 1_000_000;

/// A gateway that takes each authorization over and runs fbc-oms's check at submit
/// immediately before it would encode, as a live gateway does (decisions 0060, 0152), and
/// records what it would have written; a refused one is reported
/// `NotSent(StaleAuthorization)`, as fbc-runtime's order-entry session reports it (0062).
#[derive(Default)]
struct Recording {
    written: Vec<VenueCommand>,
    refused: Vec<StaleAuthorization>,
    next: u64,
}

impl Recording {
    /// Submits `auth` as a consumer does.
    fn send(&mut self, auth: Authorization) -> SubmitHandle {
        let ctx = EncodeCtx {
            wall: WallNs(0),
            mono: MonoNs(0),
            nonces: NonceBlock::EMPTY,
        };
        self.submit(auth, &ctx, &mut PathStamps::off())
    }
}

impl OrderGateway for Recording {
    fn submit(
        &mut self,
        auth: Authorization,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
    ) -> SubmitHandle {
        self.next += 1;
        let rpc = RpcId(self.next);
        let auth = auth.hand_over();
        let receipt = match auth.check_at_submit() {
            Ok(()) => {
                self.written.push(auth.command().clone());
                Ok(EncodeReceipt::new())
            }
            Err(why) => {
                self.refused.push(why);
                Err(NotSentReason::StaleAuthorization)
            }
        };
        SubmitHandle { rpc, receipt }
    }

    fn submit_control(
        &mut self,
        _acct: AccountKey,
        _cmd: ControlCommand,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
    ) -> SubmitHandle {
        unreachable!("these tests submit no control command")
    }
}

/// A named registry under the inventory cap on `INST` and `OTHER`, both seeded by hand at
/// `pos`, `INST` then started: armed, Quoting. It is built for a declared owner-assisted
/// testnet run (decision 0067), the one registry a hand seed arms, so every fill after the seed
/// moves the position, as on the `testnet_trade` sample's (no snapshot holds one by absence).
fn quoting(pos: i64) -> Registry {
    let caps = [INST, OTHER]
        .into_iter()
        .try_fold(PreTradeCaps::new(), |caps, m| {
            caps.with_market(
                m,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(WIDE)),
                },
            )
        })
        .unwrap();
    let mut reg =
        arm::named(Registry::with_caps(caps).for_testnet_run(TestnetRun::owner_assisted()));
    for m in [INST, OTHER] {
        reg.seed_position(m, SignedLots(pos)).unwrap();
    }
    start(&mut reg, INST);
    reg
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1_000,
        },
        WallNs(0),
    )
    .unwrap()
}

/// A venue that amends price and total by venue id, keeping the venue id.
fn amending() -> OrderCaps {
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
        ..order_caps()
    }
}

fn buy(qty: i64) -> NewOrder {
    placement(cid(), 100, qty)
}

fn key(n: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest: n,
    }
}

/// Acknowledges the placement of `c` under the venue id `v`: Open.
fn ack(reg: &mut Registry, c: ClientOrderId, v: &str) {
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
}

/// Places `order`, hands its authorization to a gateway that wrote it, and acknowledges it
/// under the venue id `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    arm::place_issued(reg, order);
    ack(reg, c, v);
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    c
}

/// Applies `f`, arriving at `MonoNs(1)`.
fn apply(reg: &mut Registry, l: &mut FillLedger, f: &FillEvent) -> FillRouted {
    match l.admit(f, None, MonoNs(1)) {
        Admission::Apply(a) => reg.apply_fill(a).unwrap(),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// A fill the side's counted orders did not already hold.
#[derive(Copy, Clone, Debug)]
enum Unheld {
    /// An own fill under our namespace's client id of an order the registry does not hold.
    Untracked,
    /// A change the venue made itself to the position (decision 0120).
    Venue,
    /// A late fill of an order the registry already ended.
    LateOnEnded,
}

const UNHELD: [Unheld; 3] = [Unheld::Untracked, Unheld::Venue, Unheld::LateOnEnded];

/// Prepares `reg` for an unheld fill of `kind` and returns it, a buy of `qty`: for a late fill,
/// an order of ours of `qty` placed, acknowledged and then ended cancelled with nothing filled,
/// so it no longer counts on its side.
fn unheld(reg: &mut Registry, kind: Unheld, qty: i64) -> (FillEvent, ClientOrderId) {
    match kind {
        Unheld::Untracked => {
            let stray = cid();
            (
                fill(Some(stray), ident("f-stray"), Side::Buy, qty, false),
                stray,
            )
        }
        Unheld::Venue => {
            let px = PxExact::new(100, 0);
            let liq = venue_fill(
                VenueFillKind::Liquidation,
                "LIQUIDATION:1",
                None,
                Side::Buy,
                qty,
                px,
                no_fee(),
            );
            (liq, cid())
        }
        Unheld::LateOnEnded => {
            let ended = open(reg, buy(qty), "v-ended");
            reg.apply_update(&update(Some(ended), canceled(), 0), key(10));
            assert!(reg.get(ended).unwrap().state().is_terminal());
            (
                fill(Some(ended), ident("f-late"), Side::Buy, qty, false),
                ended,
            )
        }
    }
}

/// What `kind`'s fill was routed as.
fn routed_as(kind: Unheld, routed: &FillRouted, c: ClientOrderId) -> bool {
    match kind {
        Unheld::Untracked => *routed == FillRouted::OursUntracked(c),
        Unheld::Venue => {
            *routed
                == FillRouted::Venue {
                    kind: VenueFillKind::Liquidation,
                    in_snapshot: false,
                }
        }
        Unheld::LateOnEnded => matches!(routed, FillRouted::Ours(o, _) if *o == c),
    }
}

/// One of the three builds on `reg`, authorized: "place" (a buy of 5), "batch" (two buys of 5)
/// or "amend" (the open buy `a` of 10 to 15).
fn authorized(reg: &mut Registry, kind: &str, a: ClientOrderId) -> Authorization {
    let cmd: PermittedCommand = match kind {
        "place" => reg.place(buy(5)).unwrap(),
        "batch" => {
            let plan = reg.place_batch(vec![buy(5), buy(5)]).unwrap();
            assert!(plan.refused.is_empty(), "{:?}", plan.refused);
            plan.command.unwrap()
        }
        "amend" => reg
            .live(a)
            .unwrap()
            .amend(&amending(), Ticks(100), lots(15), false)
            .unwrap(),
        other => unreachable!("{other}"),
    };
    reg.authorize(ACCT, cmd).unwrap()
}

/// The done line's fbc-oms half: on a Quoting market, flat, a place, a batch and an amend are
/// each authorized, then a fill the side's counted orders did not already hold takes the
/// position to the inventory cap; each is refused at submit with nothing written, while a
/// cancel authorized before the fill is written. Built again now, the same place breaches the
/// cap: sent, the stale one would have breached I6 by its own size.
#[test]
fn a_quoting_place_batch_or_amend_authorized_before_an_unheld_fill_took_the_position_to_the_cap_is_refused_at_submit()
 {
    for unheld_kind in UNHELD {
        for kind in ["place", "batch", "amend"] {
            let mut reg = quoting(0);
            let (f, c) = unheld(&mut reg, unheld_kind, CAP);
            let a = open(&mut reg, buy(10), "v-a");
            let resting = open(&mut reg, buy(1), "v-r");
            let auth = authorized(&mut reg, kind, a);
            let cancel = match reg.cancellable(resting).unwrap().cancel(&order_caps()) {
                CancelChoice::Send(cmd) => reg.authorize(ACCT, cmd).unwrap(),
                other => panic!("expected a cancel to send, got {other:?}"),
            };
            let mut l = ledger();
            let routed = apply(&mut reg, &mut l, &f);
            assert!(
                routed_as(unheld_kind, &routed, c),
                "{unheld_kind:?}: {routed:?}"
            );
            assert_eq!(reg.position(INST), Some(SignedLots(CAP)), "{unheld_kind:?}");

            let mut gw = Recording::default();
            let refused = gw.send(auth);
            assert_eq!(
                refused.receipt,
                Err(NotSentReason::StaleAuthorization),
                "{unheld_kind:?} {kind}"
            );
            assert_eq!(gw.refused, [StaleAuthorization::PositionMoved(INST)]);
            assert!(
                gw.written.is_empty(),
                "{unheld_kind:?} {kind}: nothing written"
            );
            assert!(gw.send(cancel).receipt.is_ok(), "a cancel always passes");
            assert!(
                matches!(gw.written[..], [VenueCommand::Cancel(_)]),
                "{:?}",
                gw.written
            );

            // Judged now, a buy breaches the cap the fill reached.
            assert!(
                matches!(
                    reg.place(buy(5)),
                    Err(OmsError::Capped(CapRefusal::InventoryCap { .. }))
                ),
                "{unheld_kind:?} {kind}"
            );
        }
    }
}

/// Any move of the position refuses them, a sell that made a buy safer and a move of one lot
/// included: the owner chose to refuse every one and build again (decision 0153).
#[test]
fn any_fill_that_moved_the_position_since_the_authorization_refuses_a_quoting_place_batch_or_amend()
{
    for (side, qty) in [(Side::Sell, 3), (Side::Buy, 1)] {
        for kind in ["place", "batch", "amend"] {
            let mut reg = quoting(0);
            let a = open(&mut reg, buy(10), "v-a");
            let auth = authorized(&mut reg, kind, a).hand_over();
            assert_eq!(auth.check_at_submit(), Ok(()), "{kind}: before the fill");
            let mut l = ledger();
            let f = fill(Some(cid()), ident("f-any"), side, qty, false);
            assert!(matches!(
                apply(&mut reg, &mut l, &f),
                FillRouted::OursUntracked(_)
            ));
            assert_eq!(
                auth.check_at_submit(),
                Err(StaleAuthorization::PositionMoved(INST)),
                "{side:?} {qty} {kind}"
            );
        }
    }
}

/// The watch is taken as the command is authorized, where 0082 judged it again: one built
/// before a fill and authorized after it was judged against the moved position and passes at
/// submit; one the fill took past the cap was refused at authorization instead.
#[test]
fn a_quoting_command_built_before_a_fill_and_authorized_after_it_is_judged_then_and_passes_at_submit()
 {
    for kind in ["place", "batch", "amend"] {
        let mut reg = quoting(0);
        let a = open(&mut reg, buy(10), "v-a");
        let cmd: PermittedCommand = match kind {
            "place" => reg.place(buy(5)).unwrap(),
            "batch" => reg
                .place_batch(vec![buy(5), buy(5)])
                .unwrap()
                .command
                .unwrap(),
            _ => reg
                .live(a)
                .unwrap()
                .amend(&amending(), Ticks(100), lots(15), false)
                .unwrap(),
        };
        let mut l = ledger();
        let f = fill(Some(cid()), ident("f-before"), Side::Buy, 20, false);
        assert!(matches!(
            apply(&mut reg, &mut l, &f),
            FillRouted::OursUntracked(_)
        ));
        let auth = reg.authorize(ACCT, cmd).unwrap().hand_over();
        assert_eq!(
            auth.check_at_submit(),
            Ok(()),
            "{kind}: judged at authorization"
        );
    }
    // Past the cap, refused as it is authorized (decision 0082), never reaching a gateway.
    let mut reg = quoting(0);
    let cmd = reg.place(buy(5)).unwrap();
    let mut l = ledger();
    let f = fill(Some(cid()), ident("f-cap"), Side::Buy, CAP, false);
    apply(&mut reg, &mut l, &f);
    assert!(matches!(
        reg.authorize(ACCT, cmd),
        Err(fbc_oms::IssueRefusal::Capped(_))
    ));
}

/// A fill that moves no inventory of the market, another namespace's or another market's,
/// leaves a Quoting authorization passing.
#[test]
fn a_fill_that_moves_no_inventory_of_the_market_leaves_a_quoting_authorization_passing() {
    for kind in ["place", "batch", "amend"] {
        let mut reg = quoting(0);
        let a = open(&mut reg, buy(10), "v-a");
        let auth = authorized(&mut reg, kind, a).hand_over();
        let mut l = ledger();
        let mut foreign = fill(Some(cid()), ident("f-foreign"), Side::Buy, CAP, false);
        foreign.origin = FillOrigin::Order {
            cid: Some(CidMatch::Foreign(Namespace::new(99))),
            px: Ticks(100),
        };
        assert!(matches!(
            apply(&mut reg, &mut l, &foreign),
            FillRouted::Foreign(_)
        ));
        let mut other = fill(Some(cid()), ident("f-other"), Side::Buy, CAP, false);
        other.inst = OTHER;
        assert!(matches!(
            apply(&mut reg, &mut l, &other),
            FillRouted::OursUntracked(_)
        ));
        assert_eq!(reg.inventory(INST), SignedLots(0));
        assert_eq!(reg.inventory(OTHER), SignedLots(CAP));
        assert_eq!(auth.check_at_submit(), Ok(()), "{kind}: nothing moved INST");
    }
}

/// A fill the seed already held moves no inventory of the market, so it leaves a Quoting
/// authorization passing: on a market seeded by a resync read at one instant (decision 0055),
/// an own fill of an order the registry does not hold, which no shown order or count places
/// after the snapshot, is held by the seed.
#[test]
fn a_fill_the_seed_already_held_leaves_a_quoting_authorization_passing() {
    for kind in ["place", "batch", "amend"] {
        let caps = PreTradeCaps::new()
            .with_market(
                INST,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(WIDE)),
                },
            )
            .unwrap();
        let mut reg = arm::named(Registry::with_caps(caps));
        arm::seed(&mut reg, &[(INST, 0)]);
        start(&mut reg, INST);
        let a = open(&mut reg, buy(10), "v-a");
        let auth = authorized(&mut reg, kind, a).hand_over();
        let mut l = ledger();
        let stray = cid();
        let held = fill(Some(stray), ident("f-held"), Side::Buy, CAP, false);
        assert_eq!(
            apply(&mut reg, &mut l, &held),
            FillRouted::InSnapshot(stray)
        );
        assert_eq!(reg.position(INST), Some(SignedLots(0)));
        assert_eq!(
            auth.check_at_submit(),
            Ok(()),
            "{kind}: the seed held the fill"
        );
    }
}

/// An Exit command keeps the watch its build took (decision 0066): a fill between its build and
/// its authorization refuses it at submit, though 0082's judgement at authorization admits it.
#[test]
fn an_exit_command_built_before_a_fill_and_authorized_after_it_is_still_refused_at_submit() {
    // Long 10, Quoting and armed: Flatten moves it into Exit with no lease given again.
    let mut reg = quoting(10);
    reg.flatten(INST, fbc_oms::Leases::none()).unwrap();
    let exit = NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(101) },
        reduce_only: true,
        reducing: true,
        ..placement(cid(), 101, 5)
    };
    let cmd = reg.place(exit).unwrap();
    let mut l = ledger();
    let f = fill(Some(cid()), ident("f-exit"), Side::Sell, 2, false);
    assert!(matches!(
        apply(&mut reg, &mut l, &f),
        FillRouted::OursUntracked(_)
    ));
    assert_eq!(reg.position(INST), Some(SignedLots(8)));
    let auth = reg.authorize(ACCT, cmd).unwrap().hand_over();
    assert_eq!(
        auth.check_at_submit(),
        Err(StaleAuthorization::PositionMoved(INST))
    );
}

/// A client-id mint of its own under a namespace lease of its own.
fn mint() -> CidMint {
    let lease =
        NamespaceLease::acquire(&common::lease_dir(), AccountKey::new(3), Namespace::new(41))
            .unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

/// A post-only GTC limit quote on the public book of `qty` at `px`.
fn quote(px: i64, qty: i64) -> DesiredQuote {
    DesiredQuote {
        px: Ticks(px),
        qty: lots(qty),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        tag: 0,
        reduce_fallback: ReduceFallback::KeepReduceOnly,
    }
}

/// The cost the owner accepted (decision 0153): a fill during a pass refuses the rest of that
/// pass's ladder. The planner's pass authorizes two bids; the first is written, a fill moves the
/// position, and the second is refused at submit and reported not sent. The planner's next pass
/// places that level again against the moved position, and it is written.
#[test]
fn a_fill_during_a_pass_refuses_the_rest_of_its_ladder_and_the_next_pass_places_it_again() {
    let mut reg = quoting(0);
    let mut mint = mint();
    let mut planner = ExecutionPlanner::new(
        PlannerConfig::new(Bps(2.0), lots(2), Duration::from_millis(100)).unwrap(),
    );
    let desired = DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(10_000, 5))
        .with(Side::Buy, 1, quote(9_990, 5));
    let caps = order_caps();
    let plan = planner
        .plan(
            &desired,
            &mut reg,
            &venue::of(&caps),
            ACCT,
            &mut mint,
            MonoNs(0),
        )
        .unwrap();
    assert_eq!(plan.commands.len(), 2);
    let mut gw = Recording::default();
    let mut commands = plan.commands.into_iter();
    let first = commands.next().unwrap();
    assert!(gw.send(first.auth).receipt.is_ok());
    ack(&mut reg, first.cid, "v-level-0");

    let mut l = ledger();
    let f = fill(Some(cid()), ident("f-pass"), Side::Buy, 3, false);
    assert!(matches!(
        apply(&mut reg, &mut l, &f),
        FillRouted::OursUntracked(_)
    ));

    let second = commands.next().unwrap();
    let handle = gw.send(second.auth);
    assert_eq!(handle.receipt, Err(NotSentReason::StaleAuthorization));
    assert_eq!(gw.refused, [StaleAuthorization::PositionMoved(INST)]);
    assert_eq!(gw.written.len(), 1);
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: None,
    };
    let not_sent = SubmitOutcome::NotSent(NotSentReason::StaleAuthorization);
    reg.on_outcome(second.cid, OrderOp::Place, &item, &not_sent, MonoNs(2))
        .unwrap();
    assert_eq!(
        reg.get(second.cid).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
    );

    // The next pass builds the refused level again, and it goes out.
    let again = planner
        .plan(
            &desired,
            &mut reg,
            &venue::of(&caps),
            ACCT,
            &mut mint,
            MonoNs(3),
        )
        .unwrap();
    let placed: Vec<_> = again
        .commands
        .iter()
        .map(|p| {
            (
                p.side,
                p.level,
                matches!(p.auth.command(), VenueCommand::Place(_)),
            )
        })
        .collect();
    assert_eq!(placed, [(Side::Buy, 1, true)]);
    for p in again.commands {
        assert!(gw.send(p.auth).receipt.is_ok());
    }
    assert_eq!(gw.written.len(), 2);
}
