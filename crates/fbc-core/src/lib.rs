//! The fueledbychai-rs contract: the value types every other crate and venue adapter speaks.
//!
//! So far it holds time ([`time`]), units and exact prices ([`units`]), price grids
//! ([`grid`]), sealed ids ([`ids`]) with the canonical client-id codec ([`cid`]),
//! restart-safe minting under a namespace lease ([`mint`]), fees with one sign convention and
//! per-account fee rates ([`fee`]), the decode scope that alone builds venue ids, venue
//! symbols and fees ([`scope`]), instrument specs with maker-safe quantization
//! ([`instrument`]) and venue capabilities with every field mandatory ([`caps`]); decisions
//! 0003, 0004 and 0008 fix their shape.

pub mod caps;
pub mod cid;
pub mod fee;
pub mod grid;
pub mod ids;
pub mod instrument;
pub mod mint;
pub mod scope;
pub mod time;
pub mod units;

pub use caps::{
    AckModel, AmendAck, AmendCaps, AmendQty, Batch, BookCaps, Cadence, CancelOnDisconnect, CapTag,
    ConnTopology, Continuity, Encoding, Feature, FeedSource, FillCaps, FillSource, FundingCaps,
    LimitScope, MatchingCaps, MdCaps, NonceScope, OpKind, OrderCaps, OrderKindTag, OrderingKey,
    QueueModelQuality, RateLimit, Readiness, RefKind, SeqDomain, SnapshotSource, SpeedBump,
    SpeedBumpScope, StpScope, Support, TagSet, TifTag, TouchSourceCaps, TradeCaps, VenueCaps,
};

pub use cid::{Charset, ClientIdFormat, MAX_WIRE_LEN, WireCid, decode_cid, encode_cid};
pub use fee::{
    Fee, FeeBook, FeeEntry, FeeError, FeeKey, FeeLookup, FeeRate, FeeSchedule, FeeSource,
    VenueFeeSign,
};
pub use grid::{GridError, PriceGrid};
pub use ids::{
    AccountKey, CidMatch, ClientOrderId, FillId, IdError, InstrumentId, MAX_VENUE_ID_LEN,
    Namespace, OrderRef, UnderlyingId, VenueId, VenueOrderId, VenueSymbol,
};
pub use instrument::{
    FundingSpec, InstrumentKind, InstrumentSpec, QtyError, QuantizeError, SizeStep, TradingStatus,
    VenueNativeId,
};
pub use mint::{CidMint, LeaseError, LeaseIo, NamespaceLease};
pub use scope::{DecodeScope, dispatch};
pub use time::{ConnKey, ExchNs, ExchTsKind, KernelRxNs, MonoNs, Stamp, WallNs};
pub use units::{
    Aggressor, AssetSym, BookSide, Bps, Channel, Liquidity, Lots, Money, PxExact, Side, SignedLots,
    Ticks,
};
