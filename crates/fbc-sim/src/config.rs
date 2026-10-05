//! SimVenue's configuration, every value the consumer's (decisions 0001, 0009).

use core::time::Duration;
use std::collections::BTreeMap;

use fbc_core::{
    AccountKey, BookId, ExecCaps, FeeBook, InstrumentId, MatchingCaps, SpecTable, StreamId,
};

use crate::queue::QueueConfig;

/// How long the simulated stream takes each way. No `Default`: the consumer measures it for
/// the venue SimVenue stands in for.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SimLatency {
    /// From a command's encode to the simulated venue acting on it.
    pub to_venue: Duration,
    /// From the venue acting (on a command, or on a trade that fills a resting order) to the
    /// answer reaching the codec.
    pub to_client: Duration,
}

/// What one SimVenue (its codec and its engine) is built from. No `Default`: every value is
/// the consumer's configuration of the venue it stands in for (0001, 0009).
#[derive(Clone, Debug)]
pub struct SimConfig {
    /// The order and fill capabilities of the venue it stands in for: the codec refuses what
    /// they do not offer, spells client ids in their format, and the engine reports fees in
    /// their fee sign.
    pub exec: ExecCaps,
    /// How the venue it stands in for matches: the codec places nothing for a venue with a
    /// speed bump, which the engine does not model yet (FBC-7y8).
    pub matching: MatchingCaps,
    pub latency: SimLatency,
    /// How long the runtime waits for an answer to each command before it is Unknown.
    pub rpc_timeout: Duration,
    /// The simulated order-entry stream the codec writes its frames on.
    pub stream: StreamId,
    /// The queue model's bracket.
    pub queue: QueueConfig,
    /// The simulated account the fee book's rates are looked up under.
    pub account: AccountKey,
    /// The account's fee rates; a fill's fee is its rate times its notional.
    pub fees: FeeBook,
    /// The instruments the venue lists.
    pub specs: SpecTable,
    /// Each instrument's trading book: the book channel orders queue on and cross against.
    pub books: BTreeMap<InstrumentId, BookId>,
}
