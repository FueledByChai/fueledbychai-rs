//! The conformance toy venue (BT-502; decision 0044): one venue that declares order
//! capabilities and exercises each through [`ExecCodec`] alone, so the named suite has a venue
//! that passes it and the runtime's order-entry tests have one toy, used by path, never copied.
//!
//! It uses `fbc_core` only and names no path inside this crate (only `super::` between its own
//! files), so another crate's tests can include it with
//! `#[path = "../../fbc-conformance/src/toy/mod.rs"] mod toy;`.
//!
//! So far (FBC-7lx) it is order entry: [`ToyExec`] encodes every [`VenueCommand`] kind, checking
//! the kind, time in force, channel and flags of every order against [`caps`] before it signs
//! anything, and [`ToySigner`] signs places, amends and cancels seeing only the reference the
//! request carries. It decodes nothing yet: order updates and fills arrive with FBC-7ce, queries
//! answered, resync and authentication with FBC-sal, market data with FBC-u1d and FBC-z2s.
//!
//! Protocol: its own, describing no real venue, as `fbc-core`'s toy: one record per line,
//! `kind|key=value|...`. A request's first record names its `rpc`; a batch's first record
//! counts its items (`n`), one record per item following, each numbered (`i`). Prices are
//! ticks, sizes lots, `ts` the signature time in nanoseconds from the encode context.

mod exec;
mod signer;

use core::num::NonZeroU32;
use core::time::Duration;

use fbc_core::{
    AckModel, AmendAck, AmendCaps, AmendQty, AssetSym, Batch, CancelBatch, CancelOnDisconnect,
    Channel, Charset, ClientIdFormat, ConnTopology, DecodeScope, Encoding, ExecCaps, FeedSource,
    FillCaps, FillSource, FundingCaps, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec,
    LimitScope, Lots, MatchingCaps, MdCaps, Namespace, NonceScope, OpKind, OrderCaps, OrderKindTag,
    OrderingKey, PriceGrid, RateLimit, Readiness, RefKind, SizeStep, SnapshotSource, SpecTable,
    StpScope, StreamId, Support, TagSet, TifTag, TradeCaps, TradingStatus, UnderlyingId, VenueCaps,
    VenueFeeSign, VenueId, WallNs, dispatch,
};
use rust_decimal::Decimal;

pub use exec::ToyExec;
pub use signer::ToySigner;

/// The order-entry stream every request is written to.
pub const EXEC_STREAM: StreamId = StreamId(1);
/// How long a request waits for its answer before it is `Unknown`.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// The namespace the toy's client ids and decode scope are minted under.
pub const OWN_NS: Namespace = Namespace::new(5);
/// The toy's two instruments, so a batch can span them.
pub const INST_A: InstrumentId = InstrumentId::new(7);
pub const INST_B: InstrumentId = InstrumentId::new(8);
/// Their venue symbols.
pub const SYMBOL_A: &str = "TOYA-PERP";
pub const SYMBOL_B: &str = "TOYB-PERP";
/// The longest batch of placements or cancels the toy takes.
pub const MAX_BATCH: u16 = 4;
/// The dead-man timer cancel-on-disconnect arms and each refresh restarts.
pub const DEAD_MAN_TTL: Duration = Duration::from_secs(10);

/// What the toy declares. Order entry is what its codec exercises so far; the fill fields are
/// what FBC-7ce decodes, and it declares no market data yet.
pub fn caps() -> VenueCaps {
    let by_venue_or_nonce = TagSet::of(&[RefKind::Venue, RefKind::PlacementNonce]);
    VenueCaps {
        exec: Some(ExecCaps {
            order: OrderCaps {
                kinds: TagSet::of(&[OrderKindTag::Limit]),
                tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc]),
                channels: TagSet::of(&[Channel::Public]),
                post_only: true,
                reduce_only: true,
                flag_conflicts: Vec::new(),
                // An amend names its order by the venue's id only, and its quantity is what is
                // left to fill.
                amend: Some(AmendCaps {
                    refs: TagSet::of(&[RefKind::Venue]),
                    price: true,
                    qty: true,
                    flags: true,
                    when_partially_filled: true,
                    reject_keeps_original: true,
                    keeps_venue_id: true,
                    ack: AmendAck::RpcReplyOnly,
                    qty_semantics: AmendQty::Remaining,
                    keeps_priority: None,
                }),
                // A cancel names its order by any of its references, tried in that order.
                cancel_refs: TagSet::of(&[
                    RefKind::Venue,
                    RefKind::Client,
                    RefKind::PlacementNonce,
                ]),
                // An order in Unknown with no venue id is queried by its placement nonce.
                query_refs: by_venue_or_nonce,
                cancel_before_ack: true,
                cancel_is_signed: true,
                batch_place: Some(Batch {
                    max_items: MAX_BATCH,
                }),
                // A batch cancel names orders by the venue's id or the placement nonce, never our
                // client id: narrower than a single cancel.
                batch_cancel: Some(CancelBatch {
                    max_items: MAX_BATCH,
                    refs: by_venue_or_nonce,
                }),
                cancel_all_account: Support::Unsupported,
                cancel_all_instrument: Support::Native,
                cancel_on_disconnect: CancelOnDisconnect::DeadMan {
                    max_ttl: DEAD_MAN_TTL,
                },
                ack: AckModel::SinglePhase,
                client_id: ClientIdFormat::Alnum {
                    max_len: 32,
                    charset: Charset::Alphanumeric,
                },
                cid_echoed_on_events: true,
                nonce_scope: NonceScope::PerAccountMonotonic,
                ordering_key: OrderingKey::VenueSeq,
                snapshot_source: SnapshotSource::Trustworthy,
                events_echo_flags: true,
                sign_cost_hint_us: 0,
            },
            fills: FillCaps {
                source: FillSource::Native,
                liquidity_flag: true,
                realized_pnl: false,
                realized_funding: false,
                fee_sign: VenueFeeSign::PositiveIsCost,
                fee_asset_reported: true,
                fill_id: true,
                replays_fills_on_reconnect: false,
            },
        }),
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            encoding: Encoding::Text,
            touch_sources: Vec::new(),
            books: Vec::new(),
            trades: TradeCaps {
                source: FeedSource::None,
                aggressor: false,
                trade_id: false,
            },
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::None,
            index: FeedSource::None,
            ts_precision: Duration::from_nanos(1),
            topology: ConnTopology::Shared {
                max_subscriptions: None,
            },
            max_conn_lifetime: None,
        },
        // One account budget counts every request the codec writes (decision 0018).
        limits: vec![RateLimit {
            scope: LimitScope::Account,
            ops: TagSet::of(&[
                OpKind::Place,
                OpKind::Amend,
                OpKind::Cancel,
                OpKind::CancelAll,
                OpKind::Query,
                OpKind::Control,
            ]),
            per: Duration::from_secs(1),
            units: 50,
        }],
        readiness_ceiling: Readiness::Record,
    }
}

/// Runs `f` in the decode scope the core lends for the toy's caps and namespace.
pub fn with_scope<R>(f: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R) -> R {
    dispatch(&caps(), OWN_NS, f)
}

/// The toy's instrument specs.
pub fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    for (id, symbol) in [(INST_A, SYMBOL_A), (INST_B, SYMBOL_B)] {
        table.insert(spec(id, symbol));
    }
    table
}

fn spec(id: InstrumentId, symbol: &str) -> InstrumentSpec {
    let usdc = AssetSym::new("USDC").expect("a valid asset symbol");
    let venue_symbol = with_scope(|scope| scope.venue_symbol(symbol));
    InstrumentSpec {
        id,
        venue: VenueId::new(9),
        venue_symbol: venue_symbol.expect("a valid venue symbol"),
        native_id: None,
        underlying: UnderlyingId::new(id.get()),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed(Decimal::new(5, 1)).expect("a positive tick"),
        quote_grid: None,
        size_step: SizeStep::new(Decimal::new(1, 3)).expect("a positive step"),
        min_size: Lots::new(1).expect("a count"),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usdc,
        settle_ccy: usdc,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    }
}

/// A charge's weight: the number of items a batch carries.
fn weight(items: usize) -> Option<NonZeroU32> {
    u32::try_from(items).ok().and_then(NonZeroU32::new)
}
