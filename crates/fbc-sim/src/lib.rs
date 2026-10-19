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
//! SimVenue, the simulated venue (BT-501; decision 0046), in two halves joined by a simulated
//! order-entry stream:
//!
//! - [`SimCodec`] is an [`ExecCodec`](fbc_core::ExecCodec), the boundary every venue's codec
//!   implements (0014, unchanged): it writes each command the stood-in venue takes (place,
//!   amend, cancel, their batches, an instrument cancel-all, a query, a cancel-on-disconnect
//!   arm or dead-man refresh) as a frame on the simulated stream and decodes the answers into
//!   outcomes, order updates, fills and query results through the
//!   [`DecodeScope`](fbc_core::DecodeScope) only.
//! - [`SimEngine`] is a pure state machine: fed the shard's market-data envelopes (it holds its
//!   own `fbc-book` books) and the codec's frames, it matches orders against the book and
//!   through the queue model and answers with frames ([`Answer`]) due after the configured
//!   [`SimLatency`]. It reads no clock. An amend resets the order's queue position unless the
//!   stood-in venue's caps declare `keeps_priority: Some(true)` (design §4.5). Orders the
//!   consumer injects ([`SimEngine::inject`]: another process's, design §10.2 step 2) queue and
//!   fill as modelled orders, so they are never size ahead of a later order, and are never
//!   answered as the consumer's own. A resync (0049) is answered in one frame: the resting
//!   orders, the position the fills imply in each instrument, and the request's wall time as
//!   the watermark.
//!   Answers are ordered by the stood-in venue's ordering key, fills carry the realized P&L
//!   and funding its fills declare, and a venue whose fills are derived from order status gets
//!   order updates in their place (0050).
//!   A crossing order meets SimVenue's own resting orders as the stood-in venue's self-trade
//!   prevention says, and what it takes from a displayed level stays taken until the book's
//!   next event there (0051).
//!   The host opens and closes the simulated stream ([`SimEngine::open`],
//!   [`SimEngine::close`]); a close loses the frames in flight on it, and the engine cancels
//!   what the stood-in venue cancels when the connection drops (per connection, behind an
//!   accepted arm) or its dead-man timer lapses unrefreshed (0097).
//!   A venue that acknowledges in two phases gets each placement accepted provisionally, then
//!   final once its `risk_reject_window` passes, or refused by a risk reject the consumer
//!   configures inside it ([`SimEngine::risk_reject`], [`RiskRefusal`]; 0099). Its amends
//!   are accepted provisionally, as Paradex's modify reply is, and settled by the amended
//!   order's event (0102), so a SimVenue built from Paradex's declared caps places, amends,
//!   cancels and fills (FBC-2zxk).
//!   Each end of a market-data connection epoch invalidates the engine's books it fed until
//!   their next snapshot, as the runtime's are ([`SimEngine::end_epoch`]; decision 0039).
//!
//! Hosted by the runtime (FBC-6mf, decision 0104), [`SimPeer`] is the venue end of an
//! in-process order-entry stream ([`fbc_runtime::OrderEntryPeer`]) over one engine: an
//! ordinary `ExecSession` built with `ExecSession::in_process` and a [`SimCodec`] writes to it
//! in place of a real venue's socket, the shard's `PeerFeed` hands it the market data, and it
//! gives the engine's answers at their due times, greeting each opened stream with a frame the
//! codec reports as the stream authenticated, since the simulated venue asks no login. Three
//! brackets are three sessions, each with its own peer, simulated account and fbc-oms.
//!
//! Every number SimVenue uses, its capabilities, latency, bracket and fee rates included, is in
//! the consumer's [`SimConfig`]. Not here yet: replay (FBC-w9g).

mod codec;
mod config;
mod engine;
mod peer;
mod queue;
mod wire;

pub use codec::SimCodec;
pub use config::{SimConfig, SimLatency};
pub use engine::{Answer, InjectedOrder, RiskRefusal, SimEngine, SimError};
pub use peer::{PeerCounters, SimPeer};
pub use queue::{
    Bracket, NewOrder, OrderKey, QueueConfig, QueueError, QueueModel, QueuePos, SimFill, TradeView,
};

#[cfg(test)]
mod tests;
