//! A held place is never stranded (decision 0084, FBC-657c; Reviewer B's RB133-1 and RB133-2
//! on PR #133).
//!
//! From its build until an authorization is issued for it, a place's or batch item's command
//! is held (decision 0082). Before FBC-657c a held place whose command was dropped (an early
//! return, `let _ =`, a place passed to `amend_not_submitted`, a command refused by another
//! registry) could never be released: a not-sent outcome is refused while it is held, the
//! Unknown ladder never escalates an order never sent, and its cancel waits for an
//! acknowledgement that never comes. Under the owner's $11 per-side resting cap one such order
//! blocks its side until restart.
//!
//! - Dropping a held place's command, unauthorized and not withdrawn, releases its orders: each
//!   ends not sent ([`NotSentReason::StaleAuthorization`]) and frees what it counted, by the
//!   registry's next mutating call (its next build, event, outcome or authorization).
//! - An outcome by client id for a held place is refused [`OmsError::NotIssued`]: not sent or
//!   refused change nothing (decision 0082); accepted or unknown, which say the venue may hold
//!   an order never authorized, also void its command: it is never authorized, and the order
//!   ends not sent.
//!
//! The values are the owner's first test values: a $50 inventory cap and an $11 resting cap a
//! side, on a synthetic market where one lot is worth $1.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use common::{cid, lots, order_caps, placement};
use fbc_core::{
    AccountKey, AckLevel, Bps, Channel, CidMint, ClientOrderId, InstrumentId, ItemRef, Lots,
    MonoNs, Namespace, NamespaceLease, NewOrder, NotSentReason, Side, SignedLots, SubmitOutcome,
    Ticks, Tif, VenueCommand, WallNs,
};
use fbc_oms::{
    CancelChoice, CapRefusal, DesiredBook, DesiredQuote, ExecutionPlanner, IssueRefusal,
    MarketCapsConfig, OmsError, OrdState, OrderOp, OutcomeApplied, PermittedCommand, PlannerConfig,
    PreTradeCaps, Registry, TerminalKind, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// The owner's first resting cap a side, in lots of $1.
const RESTING: i64 = 11;

/// A registry under the owner's first caps, flat: seeded by hand and started, declared an
/// owner-assisted testnet run (decision 0067).
fn registry() -> Registry {
    let caps = PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(CAP)),
                resting: Some(lots(RESTING)),
            },
        )
        .unwrap();
    let mut reg =
        arm::named(Registry::with_caps(caps).for_testnet_run(TestnetRun::owner_assisted()));
    reg.seed_position(INST, SignedLots(0)).unwrap();
    arm::start(&mut reg, INST);
    reg
}

fn buy(qty: i64) -> NewOrder {
    placement(cid(), 100, qty)
}

fn item(v: Option<&str>) -> ItemRef {
    ItemRef {
        idx: 0,
        cid: None,
        vid: v.map(common::vid),
    }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// How a held place ends once its command is gone unauthorized.
fn released() -> OrdState {
    OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
}

/// The resting cap refusing one more lot on the buy side.
fn side_full(reg: &mut Registry) {
    assert!(
        matches!(
            reg.place(buy(1)),
            Err(OmsError::Capped(CapRefusal::RestingCap { .. }))
        ),
        "the buy side is full"
    );
}

/// The buy side takes a place of the whole resting cap again, and it is authorized.
fn side_free(reg: &mut Registry) {
    let cmd = reg.place(buy(RESTING)).expect("the buy side is free again");
    assert!(reg.authorize(ACCT, cmd).is_ok());
}

/// The client ids of a batch command's items.
fn items(cmd: &PermittedCommand) -> Vec<ClientOrderId> {
    let VenueCommand::PlaceBatch(items) = cmd.command() else {
        panic!("a batch")
    };
    items.iter().map(|o| o.cid).collect()
}

// ---- RB133-2: dropping a held place's command releases it ----

#[test]
fn a_dropped_place_command_releases_its_order_and_its_cap_headroom() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    side_full(&mut reg);
    // An early return: the command goes out of scope, never authorized nor withdrawn.
    drop(cmd);
    side_free(&mut reg);
    assert_eq!(reg.get(c).unwrap().state(), released());
}

#[test]
fn a_place_whose_command_was_discarded_unbound_is_released() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let _ = reg.place(order);
    side_free(&mut reg);
    assert_eq!(reg.get(c).unwrap().state(), released());
}

#[test]
fn a_dropped_batch_command_releases_every_item() {
    let mut reg = registry();
    let plan = reg.place_batch(vec![buy(5), buy(6)]).unwrap();
    assert!(plan.refused.is_empty());
    let batch = plan.command.unwrap();
    let cids = items(&batch);
    side_full(&mut reg);
    drop(batch);
    side_free(&mut reg);
    for c in cids {
        assert_eq!(reg.get(c).unwrap().state(), released());
    }
}

#[test]
fn a_place_passed_to_amend_not_submitted_is_released() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    // Not an amend: nothing is released by it, and the command is consumed all the same.
    assert!(!reg.amend_not_submitted(cmd));
    side_free(&mut reg);
    assert_eq!(reg.get(c).unwrap().state(), released());
}

#[test]
fn a_place_refused_by_another_registry_is_released_in_its_own() {
    let mut ours = registry();
    let mut theirs = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = theirs.place(order).unwrap();
    assert_eq!(
        ours.authorize(ACCT, cmd).unwrap_err(),
        IssueRefusal::OtherRegistry
    );
    assert!(ours.get(c).is_none());
    side_free(&mut theirs);
    assert_eq!(theirs.get(c).unwrap().state(), released());
}

#[test]
fn a_held_place_whose_not_sent_report_was_refused_is_released_when_its_command_is_dropped() {
    // The consumer reports not sent by client id (refused while the command is held, 0082),
    // then loses the command: the order is not stranded.
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let not_sent = SubmitOutcome::NotSent(NotSentReason::Backpressure);
    assert_eq!(
        reg.on_outcome(c, OrderOp::Place, &item(None), &not_sent, MonoNs(1)),
        Err(OmsError::NotIssued(c))
    );
    // Its cancel waits for an acknowledgement that never comes: no cancel releases it.
    assert_eq!(
        reg.cancellable(c).unwrap().cancel(&order_caps()),
        CancelChoice::AwaitAck
    );
    drop(cmd);
    side_free(&mut reg);
    assert_eq!(reg.get(c).unwrap().state(), released());
    // Its order is terminal: it gets no cancel permit and no outcome revives it.
    assert!(reg.cancellable(c).is_err());
    assert_eq!(
        reg.on_outcome(c, OrderOp::Place, &item(Some("v")), &accepted(), MonoNs(2)),
        Ok(OutcomeApplied::Unchanged)
    );
    assert_eq!(reg.get(c).unwrap().state(), released());
}

#[test]
fn a_place_command_dropped_on_another_thread_is_released() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    // The join is the event the registry waits on: the drop happened before it.
    std::thread::spawn(move || drop(cmd)).join().unwrap();
    side_free(&mut reg);
    assert_eq!(reg.get(c).unwrap().state(), released());
}

#[test]
fn a_place_authorized_or_withdrawn_is_not_released_again_when_its_command_goes() {
    // Authorized: the authorization spent the command, and dropping the authorization ends
    // nothing; the gateway's outcome applies by client id.
    let mut reg = registry();
    let order = buy(5);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    drop(reg.authorize(ACCT, cmd).unwrap());
    let _ = reg.place(buy(1)).unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::PendingNew);
    reg.on_outcome(c, OrderOp::Place, &item(Some("v")), &accepted(), MonoNs(1))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    // Withdrawn with its command: it ends for the reason given, not another.
    let order = buy(1);
    let w = order.cid;
    let cmd = reg.place(order).unwrap();
    assert!(reg.place_not_submitted(cmd, NotSentReason::RateBudget));
    let _ = reg.place(buy(1));
    assert_eq!(
        reg.get(w).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::RateBudget))
    );
}

#[test]
fn a_planner_pass_quotes_a_side_a_dropped_place_had_filled() {
    // The testnet_quote loop's case (FBC-elg7): the consumer's own place of the whole resting
    // cap, its command dropped; the planner's pass then quotes the side.
    let mut reg = registry();
    drop(reg.place(buy(RESTING)).unwrap());
    let lease = NamespaceLease::acquire(&common::lease_dir(), ACCT, Namespace::new(951)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let mut p = ExecutionPlanner::new(
        PlannerConfig::new(Bps(2.0), lots(2), Duration::from_millis(100)).unwrap(),
    );
    let book = DesiredBook::new(INST).with(
        Side::Buy,
        0,
        DesiredQuote {
            px: Ticks(100),
            qty: lots(RESTING),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        },
    );
    let plan = p
        .plan(&book, &mut reg, &order_caps(), ACCT, &mut mint, MonoNs(0))
        .unwrap();
    assert!(plan.refused.is_empty(), "{:?}", plan.refused);
    assert_eq!(plan.commands.len(), 1);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(RESTING)));
}

// ---- RB133-1: an outcome for a held place is refused, and one saying the venue may hold it
// voids its command ----

#[test]
fn an_accepted_outcome_for_an_unissued_place_is_refused_and_its_command_is_never_authorized() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    assert_eq!(
        reg.on_outcome(c, OrderOp::Place, &item(Some("v")), &accepted(), MonoNs(1)),
        Err(OmsError::NotIssued(c))
    );
    // Not applied: the order never rested, never learnt the venue id, and ends not sent.
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), released());
    assert_eq!(rec.vid(), None);
    assert_eq!(reg.cid_of(&common::vid("v")), None);
    assert_eq!(
        reg.authorize(ACCT, cmd).unwrap_err(),
        IssueRefusal::Released(c)
    );
    side_free(&mut reg);
}

#[test]
fn an_unknown_outcome_for_an_unissued_place_is_refused_and_its_command_is_never_authorized() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    assert_eq!(
        reg.on_outcome(
            c,
            OrderOp::Place,
            &item(None),
            &SubmitOutcome::Unknown,
            MonoNs(1)
        ),
        Err(OmsError::NotIssued(c))
    );
    // Never on the Unknown ladder: nothing of it was sent.
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), released());
    assert_eq!(rec.unknown_since(), None);
    assert_eq!(
        reg.authorize(ACCT, cmd).unwrap_err(),
        IssueRefusal::Released(c)
    );
    side_free(&mut reg);
}

#[test]
fn an_accepted_outcome_for_one_unissued_batch_item_voids_the_whole_batch() {
    let mut reg = registry();
    let plan = reg.place_batch(vec![buy(5), buy(6)]).unwrap();
    let batch = plan.command.unwrap();
    let cids = items(&batch);
    assert_eq!(
        reg.on_outcome(cids[0], OrderOp::Place, &item(None), &accepted(), MonoNs(1)),
        Err(OmsError::NotIssued(cids[0]))
    );
    assert_eq!(
        reg.authorize(ACCT, batch).unwrap_err(),
        IssueRefusal::Released(cids[0])
    );
    for c in cids {
        assert_eq!(reg.get(c).unwrap().state(), released());
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
}

#[test]
fn the_not_issued_refusal_says_no_outcome_applies() {
    let c = cid();
    assert_eq!(
        OmsError::NotIssued(c).to_string(),
        format!("the place of {c:?} was never authorized: no outcome of it applies")
    );
}
