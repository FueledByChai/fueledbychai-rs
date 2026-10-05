//! Order truth (decision 0005): the monotone order lattice every later part of the OMS builds
//! on, each command item's outcome, and the registry of orders by client id.
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
//! Not here yet: fills, the `FillLedger` and the second fill counter (FBC-sq9), permits
//! (FBC-lrc), the pre-trade caps (FBC-2e4) and the Unknown ladder.

mod record;
mod registry;

pub use record::{
    Applied, Intent, OrdState, OrderKey, OrderOp, OrderRecord, OutcomeApplied, TerminalKind,
};
pub use registry::{OmsError, Registry, Routed};
