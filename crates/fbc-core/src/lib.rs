//! The fueledbychai-rs contract: the value types every other crate and venue adapter speaks.
//!
//! This first slice holds time ([`time`]), units and exact prices ([`units`]) and price grids
//! ([`grid`]); decisions 0004 and 0008 fix their shape.

pub mod grid;
pub mod time;
pub mod units;

pub use grid::{GridError, PriceGrid};
pub use time::{ConnKey, ExchNs, KernelRxNs, MonoNs, Stamp, WallNs};
pub use units::{
    Aggressor, AssetSym, BookSide, Bps, Lots, Money, PxExact, Side, SignedLots, Ticks,
};
