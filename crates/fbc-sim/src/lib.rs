//! Simulation mechanisms, with every number from the caller (decisions 0001, 0009): what a
//! simulated venue does with an order, never a calibration, a fitted parameter or its output.
//!
//! The queue-position fill model (BT-501; decision 0038): [`QueueModel`] keeps, for each
//! simulated resting order, where it sits in its price level's queue, and says when trades fill
//! it. The venue's true queue is not observed, so a model runs under one [`Bracket`]: a level
//! that shrinks with no trade at its price ([`QueueModel::level_cancel`]) advances an order by
//! none of the cancelled size ([`Bracket::Pessimistic`]), by its proportional share
//! ([`Bracket::Middle`]) or by all of it up to the size ahead ([`Bracket::Optimistic`]).
//!
//! - [`QueueModel::accept`] queues a new order behind its level's size on the public
//!   `fbc-book` book, less every modelled public order the model holds at that side and price
//!   (the simulation's own and injected ones alike), and only where the book knows that size;
//!   an RPI order queues behind every public order at its price, those that join later
//!   ([`QueueModel::public_join`]) included.
//! - [`QueueModel::trade`] lets a trade at an order's price consume the size ahead of it first
//!   and then fill it; a trade through its price empties the queue ahead and fills it from the
//!   trade's size (0038). An order is never filled past what remains, and each fill is
//!   reported once.
//!
//! SimVenue, the simulated venue (BT-501; decision 0043), in two halves joined by a simulated
//! order-entry stream:
//!
//! - [`SimCodec`] is an [`ExecCodec`](fbc_core::ExecCodec), the boundary every venue's codec
//!   implements (0014, unchanged): it writes each place and cancel as a frame on the simulated
//!   stream and decodes the answers into outcomes, order updates and fills through the
//!   [`DecodeScope`](fbc_core::DecodeScope) only.
//! - [`SimEngine`] is a pure state machine: fed the shard's market-data envelopes (it holds its
//!   own `fbc-book` books) and the codec's frames, it matches orders against the book and
//!   through the queue model and answers with frames ([`Answer`]) due after the configured
//!   [`SimLatency`]. It reads no clock.
//!
//! Every number SimVenue uses, its capabilities, latency, bracket and fee rates included, is in
//! the consumer's [`SimConfig`]. Not here yet: amends, batches, queries and injected orders
//! (FBC-nv2), hosting in the runtime (FBC-6mf) and replay (FBC-w9g).

mod codec;
mod config;
mod engine;
mod queue;
mod wire;

pub use codec::SimCodec;
pub use config::{SimConfig, SimLatency};
pub use engine::{Answer, SimEngine, SimError};
pub use queue::{
    Bracket, NewOrder, OrderKey, QueueConfig, QueueError, QueueModel, QueuePos, SimFill, TradeView,
};

#[cfg(test)]
mod tests;
