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
//! cumulative quantity raises the order's `cum_venue`. A change the venue made itself to the
//! position (a liquidation, a transfer, a settlement or a block trade; decision 0120) states
//! that origin, names no client id, and counts once on the inventory through the same ledger
//! and path, on no order, even one its venue id names, and puts nothing in view
//! ([`FillRouted::Venue`]). An order's filled quantity is
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
//! reduces the position by its formula, and an order carrying the venue's reduce-only flag
//! whenever it strictly shrinks the side's worst case, even past the cap (decision 0112,
//! FBC-ugto); the resting cap bounds it like any other. Cancels are never capped
//! (`tests/caps.rs`).
//!
//! Resyncs (decision 0055, `tests/resync.rs`): the consumer hands each resync's answer to
//! [`Registry::resync`] as a [`ResyncSnapshot`]. The first one from a trustworthy snapshot
//! source seeds each market's position, once per process; every one registers our open orders
//! it shows on a market not yet seeded that the registry does not hold (an earlier run's, never
//! amended), so a cancel reaches them. None seeds a market on which its snapshot shows an open
//! order with no client id under a venue id no order the registry holds had (one of ours a
//! retention pass forgot included, decision 0157 item 9), which nothing counts on the caps;
//! each such order is reported ([`ResyncReport::without_cid`], decision 0154). Until its
//! seed a market's position is unknown ([`Registry::position`]) and nothing is built on it. A
//! fill executed between the request and the answer counts exactly once, wherever it arrives:
//! the snapshot holds it when it arrived
//! before the request, its order shows the cumulative fill it brought, or (its order not shown)
//! it executed by the watermark or its order was at the venue before the request; a fill
//! nothing places leaves the market unknown. A change the venue made itself is placed by its
//! arrival before the request or its time by the watermark alone, and after the seed counts on
//! the inventory otherwise (decision 0120). Later resyncs compare each position with the
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
//! `tests/hand_seed.rs`), but for a flat hand seed on the one market a declared
//! owner-supervised probe run names ([`CodProbeRun`], [`Registry::for_cod_probe`], decision 0140
//! item 6, `tests/cod_probe.rs`), where Start, Flatten and Wind-down arm it after a fill moved
//! the position off it, but under a place stages' declaration
//! ([`CodProbeRun::owner_supervised_places`]) Start arms only while it is flat (decision 0148
//! item 3);
//! [`Registry::disarm`] (to Cancel-only, the leases dropped), [`Registry::kill`] and
//! [`Registry::lift_kill`] (to Cancel-only, never further). Every change advances the market's
//! [`StateGeneration`]; no resync, fill or timer changes a market's state.
//!
//! Cancels and fills in every state (decision 0012; 0005's I4 and I7; `tests/sweep.rs`): the
//! kill switch's cancel everything for a market ([`Registry::cancel_everything`]) is an
//! instrument cancel-all only while the registry holds the market's exclusive lease, a
//! trustworthy resync has shown the account's open orders, requested after the latest
//! order-entry epoch end the consumer reported ([`Registry::order_entry_epoch_ended`], called
//! from each `ExecHandler::on_epoch_end` with the ended epoch's last envelope's `recv_mono`;
//! FBC-9svb), and no order not ours is in view on the market ([`Registry::foreign_in_view`]);
//! otherwise our orders on it are cancelled by explicit reference in a cancel-many
//! ([`CancelEverything`], [`CancelAllRefusal`]), and no foreign-namespace order is ever
//! cancelled. Each resync names the Killed markets, whose
//! cancel everything goes out again ([`ResyncReport::cancel_everything`]). No account cancel-all
//! is ever built, and an instrument one is authorized only as built here. An own-namespace
//! fill moves the inventory once in every state, Killed included, as does a change the venue
//! made itself (decision 0120, `tests/venue_fills.rs`); a foreign one is flagged and moves
//! nothing.
//!
//! Orphans (0005's I7, decision 0108; `tests/orphans.rs`): open orders under our namespace that
//! the registry does not hold, which a resync finds on a market seeded before it, are
//! registered from the snapshot (from an untrustworthy source only those a live order event or
//! fill also showed), and each market holding any that is not
//! terminal is named ([`ResyncReport::orphans`]) and its adding side suppressed: a place, batch
//! item or amend is built only on the side that reduces the known, non-zero position, within its
//! size ([`OmsError::OrphanCooldown`], [`AmendRefusal::OrphanCooldown`]; judged again at
//! authorization, [`IssueRefusal::OrphanCooldown`]), until the cooldown the
//! consumer's [`Registry::cancel_orphans`] starts runs out ([`Registry::end_orphan_cooldowns`],
//! [`OrphanConfig`], no default). Their cancels go out in every market state by explicit
//! reference in a cancel-many, repeated on each resync, and above the consumer's level limit as
//! an instrument cancel-all only under the kill switch's guard ([`OrphanCancels`]); no order
//! not ours is ever one, and no account cancel-all is built.
//!
//! Orders not ours shown resting (decision 0158; `tests/not_ours_resting.rs`): while an order
//! under another namespace's, a non-canonical or no client id, under a venue id no order the
//! registry holds had or one of an order it holds as ended Lost or never sent, that an order
//! event or a snapshot row showed open rests on a market ([`Registry::not_ours_resting`], named
//! by [`Registry::not_ours_shown`] as a [`NotOursShown`]), the one pre-trade path refuses there
//! every place, batch item and amend that does not reduce the known, non-zero position within
//! its size or carry the venue's reduce-only flag on the reducing side
//! ([`OmsError::NotOursResting`], [`AmendRefusal::NotOursResting`]; judged again at
//! authorization, [`IssueRefusal::NotOursResting`]), after the market's state and an orphan
//! cooldown, and one authorized before such an order came to be shown resting that the refusal
//! would have refused is refused at submit ([`StaleAuthorization::NotOursResting`]). No cancel
//! reads it, every order Exit admits passes it, and it moves no market state. It lifts by the
//! order's end under its venue id, by a trustworthy resync requested at least the venue's lag
//! bound after its stamp (the next order-entry epoch end or retention pass reported after it was
//! last shown, never earlier than the latest end already reported) that does not list it open
//! ([`ResyncReport::not_ours_lifted`]), or by its venue id coming to be held as an order of ours
//! not ended Lost or never sent. A fill shows nothing resting.
//!
//! Authorizations and their check at submit (decisions 0045, 0060, 0066 and 0153;
//! `tests/authorization.rs`, `tests/exit_submit.rs`, `tests/quoting_submit.rs`):
//! [`Registry::authorize`] issues the [`Authorization`] for a command this crate built, so a
//! command the market's state or a cap refused never receives one, and none is ever issued for an
//! account cancel-all. It carries the market's [`StateGeneration`] at the command's build, and the
//! gateway takes it over ([`Authorization::hand_over`]) and runs [`Handed::check_at_submit`]
//! immediately before encoding: a place, a batch or an amend whose market changed state since (the
//! kill switch, a disarm, an arming call, lease names given again) is refused with nothing written,
//! and one built in Exit, or during an orphan cooldown, also once a fill or a seed moved its
//! market's inventory since (its position revision, decisions 0066 and 0108), and any other once
//! one moved it since its authorization, where the caps were judged against it (decision 0153);
//! an instrument cancel-all only once 0005's I7 guard it was built under moved (the registry's
//! hold of the market's exclusive lease changed, or an order not ours was seen on the market),
//! never for the kill switch alone; and a cancel or a cancel-many always goes through. A
//! place's, a batch's or an amend's authorization holds its orders, or the amend's build, until
//! a gateway takes it over:
//! dropped before then, they are released at the registry's next mutating call, or at once by
//! [`Registry::withdraw`], so none is stranded; only the handed form runs the check at submit, so a
//! gateway that keeps decision 0060 writes only a handed authorization, and its drop releases
//! nothing (decision 0152, `tests/dropped_authorization.rs`). The unhanded form's command stays
//! readable: a gateway that skipped the check could write it, which 0060 forbids and no type
//! prevents.
//!
//! A command is bound to the registry that built it and a registry to one account (decision
//! 0082; `tests/retained.rs`): only that registry authorizes it, for the account the registry
//! was bound to ([`Registry::bind_account`]) or else first authorized for ([`IssueRefusal`]),
//! and a place, a batch or an amend is judged against the caps again as it is authorized, so
//! one retained while a fill moved the position is refused, its reservation released. A place
//! or batch item built and not yet authorized is held: withdrawn with its command
//! ([`Registry::place_not_submitted`]) or, its command dropped, released by the registry's next
//! mutating call, so it is never stranded; no outcome of it by client id applies
//! ([`OmsError::NotIssued`]), and an accepted or unknown one voids its command (decision 0084,
//! `tests/held.rs`). An amend's build token names its registry, so another registry's command
//! releases nothing ([`Registry::amend_not_submitted`]).
//!
//! Retention (decision 0157; `tests/retention.rs`): a registry forgets a terminal order only at
//! the consumer's pass, [`Registry::retain_ended`], under its [`RetentionConfig`] (a count, an
//! age and a resync bound, none zero, no default), at the consumer's time on the shard's
//! monotonic clock. It keeps every order that is not terminal, every terminal one a pin holds
//! (ended Lost or by an accepted tombstone, owing a fill or an answer, named by a command, an
//! authorization, a handed authorization or a planner slot, on a market not seeded, inserted
//! after a resync's seed, or sharing a venue id with another order held), and every one whose
//! stamp is younger than the venue's snapshot lag bound plus the resync bound; of the others it
//! keeps at most the count, none past the age, and reports each kind ([`Retained`],
//! [`Pinned`]). A forgotten order leaves two spans per namespace behind: a place or an insert
//! under a client id the built span covers is refused ([`OmsError::DuplicateCid`]), a late fill
//! under one is placed as its held record's fill would be, and an outcome or a sent report under
//! a client id the answer span covers gets a terminal record's answer. A registry whose consumer
//! never runs the pass keeps every order.
//!
//! The audit sink (decision 0005's I8, library form; `tests/audit.rs`): every raw order event
//! [`Registry::apply_update`] receives is handed to the consumer's [`AuditSink`]
//! ([`Registry::set_audit_sink`]) in receive order, numbered, before any of it is applied, the
//! ones then ignored as late, stale or for a superseded venue id and those routed to no order
//! of ours included, and its verdict follows it before the next event is received
//! ([`AuditEntry`]); a cancel's reason is the venue's, as the event states it, and a verdict
//! on our order carries the consumer's tag and removal reason the execution planner gave it
//! ([`OrderRecord::tag`], [`OrderRecord::removal`]; `tests/tags.rs`). Writing it to
//! the consumer's `order_audit` table, that table's schema and the legacy status names stay
//! with the consumer (decision 0001).
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
//! [`OrderCaps`](fbc_core::OrderCaps) admit it, otherwise, or where an order not ours is shown
//! resting on the market and 0158's refusal would refuse the order as it rests, a cancel, the
//! level then waiting for the order's terminal state before the new one is placed (an amend
//! refused at its build is reported, never followed by a cancel); a level whose order is
//! PendingNew or Unknown, or has a command in flight, is occupied until it settles
//! ([`HeldReason`]); a place or amend the market's state or a cap refuses is never built
//! ([`PlanRefusal`]). Each level carries the consumer's opaque tag ([`DesiredQuote::tag`])
//! and each side an optional removal reason ([`DesiredBook::removal`]): every place and amend
//! built for a level carries its tag, every cancel of a level the book no longer wants its
//! side's reason ([`Planned`]), and the order keeps both for the audit sink's verdicts;
//! fbc-oms gives them no meaning (decisions 0001, 0098; `tests/tags.rs`). Each pass takes the
//! venue's facts as the consumer states them, with no default ([`PlanVenue`]; decision 0111;
//! `tests/gates.rs`): nothing the market's trading status or a venue mode refuses is built,
//! cancels excepted; normal commands stop at each rate scope's safety floor ([`RateView`]),
//! cancels and reducing orders never; and under [`Batching::Declared`] places and cancels go
//! in batches within the venue's declared sizes and references, a crossing intent and, while
//! an order not ours is shown resting on the market, a place carrying the venue's reduce-only
//! flag always alone (decision 0158 item 3).

mod audit;
mod caps;
mod entry;
mod gates;
mod gateway;
mod grant;
mod ladder;
mod ledger;
mod orphans;
mod permit;
mod planner;
mod record;
mod registry;
mod resync;
mod retain;
mod sweep;

/// The helpers the integration tests share, once for the unit tests too: one namespace lease
/// per test binary.
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

pub use audit::{AuditEntry, AuditSink};
pub use caps::{CapRefusal, CapsConfigError, MarketCaps, MarketCapsConfig, PreTradeCaps};
pub use entry::{
    ArmRefusal, CancelOnlyTrip, CodProbeRun, EntryState, ExitKind, ExitRefusal, LeaseKeys, Leases,
    MarketEntry, StateRefusal, TestnetRun,
};
pub use gates::{Batching, PlanVenue, RateScope, RateView};
pub use gateway::{ControlCommand, ManagedGateway, OrderGateway};
pub use grant::{Authorization, Handed, IssueRefusal, StaleAuthorization, StateGeneration};
pub use ladder::{LadderConfig, LadderConfigError, LadderPlan, LadderResolution, ResyncApplied};
pub use ledger::{
    AcceptedFill, Admission, FillLedger, FillTime, Horizon, LedgerConfig, LedgerConfigError,
    ReplayCounts,
};
pub use orphans::{Cooldown, OrphanCancels, OrphanConfig, OrphanConfigError, OrphanPlan};
pub use permit::{
    AmendRefusal, CancelChoice, CancelPlan, Cancellable, Live, PermitRefusal, PermittedCommand,
    PlacePlan,
};
pub use planner::{
    BatchItem, CROSSING_LEVEL, CrossingIntent, DesiredBook, DesiredQuote, Downgrade,
    ExecutionPlanner, Held, HeldReason, Plan, PlanError, PlanRefusal, Planned, PlannerConfig,
    PlannerConfigError, ReduceFallback, Refused, Stage,
};
pub use record::{
    Applied, FillApplied, Intent, LadderStep, OrdState, OrderKey, OrderOp, OrderRecord,
    OutcomeApplied, TerminalKind,
};
pub use registry::{FillRouted, OmsError, Registry, Routed};
pub use resync::{PositionCheck, ResyncError, ResyncReport, ResyncSnapshot};
pub use retain::{Pinned, Retained, RetentionConfig, RetentionConfigError};
pub use sweep::{CancelAllRefusal, CancelEverything, NotOursShown};
