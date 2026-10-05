//! Simulation mechanisms, with every number from the caller (decisions 0001, 0009): what a
//! simulated venue does with an order, never a calibration, a fitted parameter or its output.
//!
//! So far the queue-position fill model (BT-501; decision 0038): [`QueueModel`] keeps, for each
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
//! Not here yet: the simulated venue (FBC-uoo) and amends (an amend's queue reset is the
//! venue's, design §10.2).

mod queue;

pub use queue::{
    Bracket, NewOrder, OrderKey, QueueConfig, QueueError, QueueModel, QueuePos, SimFill, TradeView,
};

#[cfg(test)]
mod tests;
