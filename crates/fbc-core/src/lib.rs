//! The fueledbychai-rs contract: the value types every other crate and venue adapter speaks.
//!
//! So far it holds time ([`time`]), units and exact prices ([`units`]), price grids
//! ([`grid`]), sealed ids ([`ids`]) with the canonical client-id codec ([`cid`]),
//! restart-safe minting under a namespace lease ([`mint`]), fees with one sign convention and
//! per-account fee rates ([`fee`]), the decode scope that alone builds venue ids, venue
//! symbols and fees ([`scope`]), instrument specs with maker-safe quantization
//! ([`instrument`]) and venue capabilities with every field mandatory ([`caps`]); decisions
//! 0003, 0004 and 0008 fix their shape.
//!
//! It also fixes the boundary every venue adapter implements (decision 0002): normalized
//! market-data and execution events ([`event`]), venue commands and their outcomes
//! ([`command`]), the sans-IO codec traits with their effects and the encode context that is a
//! codec's only source of time and nonces ([`codec`]), and the venue factory and gateway
//! traits ([`venue`]).

pub mod caps;
pub mod cid;
pub mod codec;
pub mod command;
pub mod event;
pub mod fee;
pub mod grid;
pub mod ids;
pub mod instrument;
pub mod mint;
pub mod scope;
pub mod time;
pub mod units;
pub mod venue;

pub use caps::{
    AckModel, AmendAck, AmendCaps, AmendQty, Batch, BookCaps, Cadence, CancelOnDisconnect, CapTag,
    ConnTopology, Continuity, Encoding, Feature, FeedSource, FillCaps, FillSource, FundingCaps,
    LimitScope, MatchingCaps, MdCaps, NonceScope, OpKind, OrderCaps, OrderKindTag, OrderingKey,
    QueueModelQuality, RateLimit, Readiness, RefKind, SeqDomain, SnapshotSource, SpeedBump,
    SpeedBumpScope, StpScope, Support, TagSet, TifTag, TouchSourceCaps, TradeCaps, VenueCaps,
};

pub use cid::{Charset, ClientIdFormat, MAX_WIRE_LEN, WireCid, decode_cid, encode_cid};
pub use codec::{
    AmendWire, CancelWire, DecodeError, Effect, Effects, EncodeCtx, EncodeReceipt, ExecCodec,
    ExecSink, Feed, Header, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag, Keepalive,
    KeepaliveKind, MAX_SIG_LEN, MdCodec, MdSink, NonceBlock, NonceSource, OrderSigner, PlaceWire,
    RawFrame, RedactError, RpcCall, Sig, SignError, SpecTable, Subscription, TimerTag,
    TrafficClass, WireSlice,
};
pub use command::{
    AckLevel, AmendOrder, CancelOrder, CancelScope, NewOrder, NotAmendable, NotSentReason,
    OrderKind, QueryOrder, Reject, RejectKind, SubmitOutcome, TerminalHint, TerminalReject, Tif,
    VenueCommand,
};
pub use event::{
    CancelReason, ConnState, Envelope, ExecEvent, FeedHealth, FillEvent, FillKey, ItemRef,
    Liquidity3, Lvl, MdEvent, ModeScope, OrderUpdate, RpcId, StreamId, TouchSourceId, VenueMeta,
    VenueMode, VenueOrderSnapshot, VenueOrderState,
};
pub use fee::{
    Fee, FeeBook, FeeEntry, FeeError, FeeKey, FeeLookup, FeeRate, FeeSchedule, FeeSource,
    PublishedRates, VenueFeeSign,
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
pub use venue::{
    ConfigError, ConfigScope, EndpointPlan, FieldSpec, FieldUnit, ManagedGateway, OrderGateway,
    SubmitHandle, VenueConfig, VenueError, VenueFactory,
};
