//! The fueledbychai-rs contract: the value types every other crate and venue adapter speaks.
//!
//! So far it holds time ([`time`]), units and exact prices ([`units`]), price grids
//! ([`grid`]), and sealed ids ([`ids`]) with the canonical client-id codec ([`cid`]),
//! restart-safe minting under a namespace lease ([`mint`]) and the decode scope that alone
//! builds venue ids ([`scope`]); decisions 0004 and 0008 fix their shape.

pub mod cid;
pub mod grid;
pub mod ids;
pub mod mint;
pub mod scope;
pub mod time;
pub mod units;

pub use cid::{Charset, ClientIdFormat, MAX_WIRE_LEN, WireCid, decode_cid, encode_cid};
pub use grid::{GridError, PriceGrid};
pub use ids::{
    AccountKey, CidMatch, ClientOrderId, FillId, IdError, MAX_VENUE_ID_LEN, Namespace, OrderRef,
    VenueOrderId,
};
pub use mint::{CidMint, LeaseError, LeaseIo, NamespaceLease};
pub use scope::{DecodeScope, dispatch};
pub use time::{ConnKey, ExchNs, KernelRxNs, MonoNs, Stamp, WallNs};
pub use units::{
    Aggressor, AssetSym, BookSide, Bps, Lots, Money, PxExact, Side, SignedLots, Ticks,
};
