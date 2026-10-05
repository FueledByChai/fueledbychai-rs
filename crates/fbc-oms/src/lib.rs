//! Order truth (decision 0005): the monotone order lattice every later part of the OMS builds
//! on, each command item's outcome, the registry of orders by client id, and the one way order
//! entry reaches a gateway.
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
//! Decision 0005's I1 (for order updates) and I2 are property-tested in `tests/lattice.rs`
//! (decision 0037).
//!
//! Order entry reaches a venue only through this crate (0013 rule 2, decision 0045): the
//! gateway traits, [`OrderGateway`] and [`ManagedGateway`], live here, and
//! [`OrderGateway::submit`] takes an [`Authorization`], which only this crate issues, for one
//! order-affecting command (a place, an amend, a batch, a cancel, a cancel-many or an
//! instrument cancel-all) on one market, carrying that market's [`StateGeneration`]. It cannot
//! be cloned or edited, and submitting consumes it (`tests/compile_fail.rs`). A command that
//! affects no order goes through [`OrderGateway::submit_control`] as a [`ControlCommand`].
//!
//! Not here yet: fills, the `FillLedger` and the second fill counter (FBC-sq9), permits
//! (FBC-lrc), the pre-trade caps (FBC-2e4), the market states (FBC-c4v), issuing an
//! authorization after them and its check at submit (FBC-afd), and the Unknown ladder.

mod gateway;
mod grant;
mod record;
mod registry;

/// The helpers the integration tests share, once for the unit tests too: one namespace lease
/// per test binary.
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

pub use gateway::{ControlCommand, ManagedGateway, OrderGateway};
pub use grant::{Authorization, StateGeneration};
pub use record::{
    Applied, Intent, OrdState, OrderKey, OrderOp, OrderRecord, OutcomeApplied, TerminalKind,
};
pub use registry::{OmsError, Registry, Routed};
