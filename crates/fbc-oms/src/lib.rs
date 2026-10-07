//! Order truth (decision 0005): the monotone order lattice every later part of the OMS builds
//! on, each command item's outcome, the registry of orders by client id, the fill ledger
//! through which alone fills move an order's fill count and the inventory, and the one way
//! order entry reaches a gateway.
//!
//! An [`OrderRecord`] holds one of our orders. Its [`OrdState`]s are ranked: PendingNew and
//! Unknown (0), Open (1), PartiallyFilled (2), Terminal (3), and a terminal state is absorbing.
//! [`OrderRecord::apply_update`] applies a venue's [`OrderUpdate`](fbc_core::OrderUpdate) under
//! its [`OrderKey`] (the venue's ordering key with the ingest order as tiebreak): a terminal
//! update always applies unless it carries a superseded venue id; a non-terminal update
//! strictly older than the last one applied is ignored; an amended update carries only its new
//! venue id, its price and total being the update's own (decision 0014 item 7); and `cum_venue`
//! is the largest cumulative fill the venue reported. [`OrderRecord::on_outcome`] applies one
//! item's [`SubmitOutcome`](fbc_core::SubmitOutcome): a placement not sent or rejected ends the
//! order, an accepted one opens it under the item's venue id, an unanswered one moves it to
//! Unknown, which counts as fully resting and is never resent, and a refused amend or cancel
//! leaves the order as it was. [`Registry`] keeps the records by client id and routes each
//! update to its order.
//!
//! Fills (decision 0005, I3) pass the [`FillLedger`] first, which deduplicates them by
//! [`FillEvent::key`](fbc_core::FillEvent::key) and holds them for an age and a count the
//! consumer configures. A live fill is accepted when absent; a replayed or snapshot fill only
//! reconciles, accepted only when absent and executed after both the session-start watermark
//! and the ledger's retention horizon, the venue time of the latest fill it forgot, so a fill
//! forgotten and replayed after a long reconnect cannot be applied twice; a refused replay is
//! counted ([`ReplayCounts`]). The ledger hands an accepted fill back as an [`AcceptedFill`],
//! which [`Registry::apply_fill`] consumes: the only path that moves an order's `cum_fills`
//! and the inventory. The ledger records a fill only once the registry counts it; it cannot
//! be cloned, and a registry takes fills from one ledger only. A fill under another
//! namespace's or a non-canonical client id, or naming neither our client id nor a venue id
//! the registry knows, is flagged and not counted (decision 0005, I4); a fill's reported
//! cumulative quantity raises the order's `cum_venue`. An order's filled quantity is
//! the larger of the venue's cumulative count (`cum_venue`) and `cum_fills`, never their sum;
//! a fill promotes a PendingNew or Unknown order, and the order is Filled only when its fills
//! alone cover every total the venue may hold, which is checked again whenever the total or
//! the amends in flight change. An amend to a larger total counts as resting from when it is
//! sent until a confirmation tied to it, or to a later amend, arrives, until the venue states
//! the total with no command in flight, or until it is refused while no earlier amend is
//! unconfirmed: an amended update stating no price or total is tied
//! to the amend in flight only by a later venue ordering key, never by arriving later or by a
//! new venue id alone, since it may duplicate, or be a late notice of, an older confirmation.
//!
//! Decision 0005's I1 (for order updates) and I2 are property-tested in `tests/lattice.rs`,
//! I1 with fills and I3 in `tests/fills.rs` (decision 0037).
//!
//! Order entry reaches a venue only through this crate (0013 rule 2, decision 0045): the
//! gateway traits, [`OrderGateway`] and [`ManagedGateway`], live here, and
//! [`OrderGateway::submit`] takes an [`Authorization`], which only this crate issues, for one
//! order-affecting command (a place, an amend, a batch, a cancel, a cancel-many or an
//! instrument cancel-all) on one market, carrying that market's [`StateGeneration`]. It cannot
//! be cloned or edited, and submitting consumes it (`tests/compile_fail.rs`). A command that
//! affects no order goes through [`OrderGateway::submit_control`] as a [`ControlCommand`].
//!
//! Amends and cancels are built through permits (decision 0005): the [`Registry`] gives a
//! [`Live`] permit for a resting order with nothing in flight, the only way to build an amend,
//! and a [`Cancellable`] one for any order that is not terminal, the only way to build a
//! cancel; never one for another namespace's or a non-canonical order (I4). A cancel names its
//! order by the reference design §4.9 orders from the venue's [`OrderCaps`](fbc_core::OrderCaps),
//! or waits for the acknowledgement and is due the moment it lands
//! ([`Registry::cancels_due`]); a cancel-many batches only the items whose reference its batch
//! declares and sends the others alone ([`Registry::cancel_many`]). What a permit builds is a
//! [`PermittedCommand`], from which alone an amend or a cancel is authorized (`tests/permits.rs`,
//! and `tests/ui_permits/` for the compile-fail half).
//!
//! The Unknown ladder (decision 0005's I5 and I9) resolves an order whose fate the OMS does not
//! know: an Unknown order, an amend or cancel unanswered or not known to the venue, or a
//! command in flight past the consumer's intent timeout ([`LadderConfig`]). On the consumer's
//! journaled timer, [`Registry::ladder`] queries it once by a reference the venue's queries
//! declare, asks for resyncs while the query is inconclusive, and tombstone-cancels it by
//! client id after the configured maximum; [`Registry::on_query_answer`] and
//! [`Registry::on_resync`] resolve it from what the venue shows, and a trustworthy snapshot
//! source that misses it the configured number of times in a row, past its sent time and the
//! settle time, ends it [`TerminalKind::Lost`], counted ([`Registry::lost`]). While on the
//! ladder it counts as fully resting and gets no [`Live`] permit; nothing places or amends it
//! again (`tests/ladder.rs`).
//!
//! The pre-trade caps (0013 rule 2) are the consumer's [`PreTradeCaps`], per market, both
//! required with no default (a [`MarketCapsConfig`] missing either is refused,
//! [`CapsConfigError`]); decision 0052 names them: the inventory cap is 0005's I6, the
//! worst-case position `|pos + Σ resting same side + new| ≤ cap`, and the resting cap a gross
//! bound per side, `Σ resting same side + new ≤ cap`, which also bounds the side that reduces
//! the position, where I6 admits up to twice the inventory cap. The position is the one a
//! resync seeded from the venue ([`Registry::resync`], [`Registry::position`]), moved by the
//! fills the ledger accepted; a market not seeded, or whose position a fill made unknown since,
//! admits nothing. Each order counts what may rest as
//! [`Registry::resting_on`] counts it (PendingNew and Unknown orders in full, a partly filled
//! order's remainder until it is terminal, an amend at the larger of its old and new quantity
//! from when it is built), and against the inventory cap its [`OrderRecord::exposure`]: that,
//! plus the fills the venue reported that the inventory does not hold yet. A place is built
//! only by [`Registry::place`], a batch only by [`Registry::place_batch`] (each item judged
//! with the earlier ones admitted counted PendingNew) and an amend or replace only by
//! [`Live::amend`], each refused, never built, when it would breach either cap or its market
//! has none, reducing and reduce-only ones included: I6 admits an order that genuinely
//! reduces the position by its formula; the resting cap bounds it like any other. Cancels are
//! never capped (`tests/caps.rs`).
//!
//! Resyncs (decision 0055, `tests/resync.rs`): the consumer hands each resync's answer to
//! [`Registry::resync`] as a [`ResyncSnapshot`]. The first one from a trustworthy snapshot
//! source seeds each market's position, once per process; every one registers our open orders
//! it shows on a market not yet seeded that the registry does not hold (an earlier run's, never
//! amended), so a cancel reaches them. Until its seed a market's position is unknown
//! ([`Registry::position`]) and nothing is built on it. A fill executed between the request and
//! the answer counts exactly once, wherever it arrives: the snapshot holds it when it arrived
//! before the request, its order shows the cumulative fill it brought, or (its order not shown)
//! it executed by the watermark or its order was at the venue before the request; a fill
//! nothing places leaves the market unknown. Later resyncs compare each position with the
//! inventory as of the watermark and report a desync, never overwriting it ([`PositionCheck`]).
//!
//! Market states and arming (decision 0012; 0013 rules 1 and 3; `tests/states.rs`): each
//! market is armed or not and in one of Killed, Cancel-only, Exit or Quoting
//! ([`EntryState`], [`Registry::entry`]); a fresh registry has every market disarmed and in
//! Cancel-only. A place, amend, replace or batch item is built only on an armed market in
//! Quoting or Exit, the market's state checked before either cap ([`StateRefusal`]): Killed
//! and Cancel-only build none, reduce-only and reducing ones included, and Exit only exit
//! orders (decision 0063, `tests/exit.rs`): reduce-only or classified reducing, on the side that reduces
//! the position, sized so that the position plus every order on that side that may still
//! move it, the new one included, never crosses zero, then judged by both caps; none once the
//! position is flat or while it is unknown, and no ordinary order ([`ExitRefusal`]). Cancels
//! are built in every state. The consumer's calls on the
//! owner's action move it: [`Registry::start`] (the only way to Quoting),
//! [`Registry::flatten`] and [`Registry::wind_down`] (straight into Exit), each arming a
//! disarmed market with its market lease, and the account lease where the venue's nonces are
//! per account, checked against the consumer's [`LeaseKeys`], and each refused, changing
//! nothing, for a Killed market, while the market's position is unknown, and for a market
//! seeded by hand outside a declared testnet run ([`ArmRefusal`], decision 0067,
//! `tests/hand_seed.rs`);
//! [`Registry::disarm`] (to Cancel-only, the leases dropped), [`Registry::kill`] and
//! [`Registry::lift_kill`] (to Cancel-only, never further). Every change advances the market's
//! [`StateGeneration`]; no resync, fill or timer changes a market's state.
//!
//! Cancels and fills in every state (decision 0012; 0005's I4 and I7; `tests/sweep.rs`): the
//! kill switch's cancel everything for a market ([`Registry::cancel_everything`]) is an
//! instrument cancel-all only while the registry holds the market's exclusive lease, a
//! trustworthy resync has shown the account's open orders and no order not ours is in view on
//! the market ([`Registry::foreign_in_view`]); otherwise our orders on it are cancelled by
//! explicit reference in a cancel-many ([`CancelEverything`], [`CancelAllRefusal`]), and no
//! foreign-namespace order is ever cancelled. Each resync names the Killed markets, whose
//! cancel everything goes out again ([`ResyncReport::cancel_everything`]). No account cancel-all
//! is ever built, and an instrument one is authorized only as built here. An own-namespace
//! fill moves the inventory once in every state, Killed included; a foreign one is flagged and
//! moves nothing.
//!
//! Authorizations and their check at submit (decisions 0045, 0060 and 0066;
//! `tests/authorization.rs`, `tests/exit_submit.rs`):
//! [`Registry::authorize`] issues the [`Authorization`] for a command this crate built, so a
//! command the market's state or a cap refused never receives one, and none is ever issued for
//! an account cancel-all. It carries the market's [`StateGeneration`] at the command's build,
//! and the gateway runs [`Authorization::check_at_submit`] immediately before encoding: a
//! place, a batch or an amend whose market changed state since (the kill switch, a disarm, an
//! arming call, lease names given again) is refused with nothing written, and one built in
//! Exit also once a fill or a seed moved its market's inventory since (its position revision,
//! decision 0066); an instrument cancel-all only once 0005's I7 guard it was built under
//! moved (the registry's hold of the market's exclusive lease changed, or an order not ours
//! was seen on the market), never for the kill switch alone; and a cancel or a cancel-many
//! always goes through.
//!
//! The execution planner (decision 0005's one planner, decisions 0065 and 0068;
//! `tests/planner.rs`): the consumer's [`DesiredBook`], a [`DesiredQuote`] per side and level,
//! defined here so strategy code produces it and this crate depends on no strategy (0001). The
//! [`ExecutionPlanner`] diffs it against the orders it placed for the account (each account's
//! apart, through the one registry it is bound to, [`PlanError`]), per side and level,
//! under the consumer's [`PlannerConfig`] (price and quantity replace thresholds and a minimum
//! age, no default), and builds what closes the difference, each command through the one pre-trade
//! path and the permits above and leaving as an [`Authorization`]: cancels, then reducing
//! orders, then amends, then adds ([`Stage`]). A change is an amend where the venue's
//! [`OrderCaps`](fbc_core::OrderCaps) admit it, otherwise a cancel, the level then waiting
//! for the order's terminal state before the new one is placed; a level whose order is
//! PendingNew or Unknown, or has a command in flight, is occupied until it settles
//! ([`HeldReason`]); a place or amend the market's state or a cap refuses is never built
//! ([`PlanRefusal`]).

mod caps;
mod entry;
mod gateway;
mod grant;
mod ladder;
mod ledger;
mod permit;
mod planner;
mod record;
mod registry;
mod resync;
mod sweep;

/// The helpers the integration tests share, once for the unit tests too: one namespace lease
/// per test binary.
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

pub use caps::{CapRefusal, CapsConfigError, MarketCaps, MarketCapsConfig, PreTradeCaps};
pub use entry::{
    ArmRefusal, EntryState, ExitKind, ExitRefusal, LeaseKeys, Leases, MarketEntry, StateRefusal,
    TestnetRun,
};
pub use gateway::{ControlCommand, ManagedGateway, OrderGateway};
pub use grant::{Authorization, IssueRefusal, StaleAuthorization, StateGeneration};
pub use ladder::{LadderConfig, LadderConfigError, LadderPlan, LadderResolution, ResyncApplied};
pub use ledger::{
    AcceptedFill, Admission, FillLedger, FillTime, Horizon, LedgerConfig, LedgerConfigError,
    ReplayCounts,
};
pub use permit::{
    AmendRefusal, CancelChoice, CancelPlan, Cancellable, Live, PermitRefusal, PermittedCommand,
    PlacePlan,
};
pub use planner::{
    DesiredBook, DesiredQuote, ExecutionPlanner, Held, HeldReason, Plan, PlanError, PlanRefusal,
    Planned, PlannerConfig, PlannerConfigError, Refused, Stage,
};
pub use record::{
    Applied, FillApplied, Intent, LadderStep, OrdState, OrderKey, OrderOp, OrderRecord,
    OutcomeApplied, TerminalKind,
};
pub use registry::{FillRouted, OmsError, Registry, Routed};
pub use resync::{PositionCheck, ResyncError, ResyncReport, ResyncSnapshot};
pub use sweep::{CancelAllRefusal, CancelEverything};
