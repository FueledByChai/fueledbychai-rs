//! The Paradex venue adapter (decision 0016): its signer ([`sign`]), the SNIP-12 revision 0
//! typed-data hash of the messages Paradex signs and their Stark-curve signature behind
//! fbc-core's `OrderSigner`; and its public market data ([`md`]): SBE frames decoded by their
//! stated block lengths into touches, trades and order book events with seq_no continuity
//! (decision 0022), behind the [`ParadexFactory`]; and its
//! authentication ([`auth`], a review path, 0009): the login signed from the consumer's
//! credentials, the session token it gives, its refresh, and Test Connection (decision 0048).
//! Order entry ([`exec`], BT-402) declares its capabilities and the SBE schema version its
//! socket negotiates (decision 0054), encodes commands as the socket's signed JSON-RPC frames
//! ([`exec::ParadexEncoder`]), and decodes the private `OrderEvent` into order updates; its
//! codec ([`exec::ParadexExec`]) is what the factory builds from the consumer's configuration
//! and credentials, or the read-only one, with one order-entry endpoint (decision 0072).

pub mod auth;
pub mod exec;
pub mod factory;
pub mod md;
pub mod sign;

pub use factory::ParadexFactory;
