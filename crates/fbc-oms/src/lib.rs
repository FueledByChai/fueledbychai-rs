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
//! the total with no amend in flight, or until it is refused while no earlier amend is
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
//! Not here yet: permits (FBC-lrc), the pre-trade caps (FBC-2e4), the market states
//! (FBC-c4v), issuing an authorization after them and its check at submit (FBC-afd), and the
//! Unknown ladder.

mod gateway;
mod grant;
mod ledger;
mod record;
mod registry;

/// The helpers the integration tests share, once for the unit tests too: one namespace lease
/// per test binary.
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

pub use gateway::{ControlCommand, ManagedGateway, OrderGateway};
pub use grant::{Authorization, StateGeneration};
pub use ledger::{
    AcceptedFill, Admission, FillLedger, FillTime, Horizon, LedgerConfig, LedgerConfigError,
    ReplayCounts,
};
pub use record::{
    Applied, FillApplied, Intent, OrdState, OrderKey, OrderOp, OrderRecord, OutcomeApplied,
    TerminalKind,
};
pub use registry::{FillRouted, OmsError, Registry, Routed};
