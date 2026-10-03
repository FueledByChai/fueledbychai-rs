//! The Paradex venue adapter (decision 0007). So far it holds only the signer ([`sign`]): the
//! SNIP-12 revision 0 typed-data hash of the messages Paradex signs and their Stark-curve
//! signature, behind fbc-core's `OrderSigner`. Market data and order entry come in later
//! tickets.

pub mod sign;
