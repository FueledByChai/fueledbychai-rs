//! The Paradex venue adapter (decision 0016): its signer ([`sign`]), the SNIP-12 revision 0
//! typed-data hash of the messages Paradex signs and their Stark-curve signature behind
//! fbc-core's `OrderSigner`; and its public market data ([`md`]): SBE frames decoded by their
//! stated block lengths into touches, trades and order book events with seq_no continuity
//! (decision 0022), behind the [`ParadexFactory`] with market-data capabilities only. Order entry comes in a later ticket (BT-402).

pub mod factory;
pub mod md;
pub mod sign;

pub use factory::ParadexFactory;
