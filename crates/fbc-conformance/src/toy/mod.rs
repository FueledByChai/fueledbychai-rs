//! The conformance toy venue (BT-502; decision 0044): one venue that declares order
//! capabilities and exercises each through [`ExecCodec`] alone, so the named suite has a venue
//! that passes it and the runtime's order-entry tests have one toy, used by path, never copied.
//!
//! It uses `fbc_core` only and names no path inside this crate (only `super::` between its own
//! files), so another crate's tests can include it with
//! `#[path = "../../fbc-conformance/src/toy/mod.rs"] mod toy;`.
//!
//! So far it is order entry (FBC-7lx): [`ToyExec`] encodes every [`VenueCommand`] kind, checking
//! the kind, time in force, channel and flags of every order against [`caps`] before it signs
//! anything, and [`ToySigner`] signs places, amends and cancels seeing only the reference the
//! request carries; and the order and fill events (FBC-7ce): [`ToyExec`] decodes order updates,
//! fills, request rejects (through [`REJECT_CODES`]) and venue modes through the
//! [`DecodeScope`] it is lent; and the answers (FBC-sal): order queries by venue id or placement
//! nonce answered with the query's rpc, a request's items answered in separate frames held and
//! pushed in one call, a resync answered in frames and pushed whole at its end, and an
//! authentication with [`TOY_TOKEN`] acknowledged; and its books (FBC-u1d): [`ToyMd`] keeps two
//! book channels apart on one connection, [`BOOK`] snapshotted in one frame decoded whole or not
//! at all and [`ANCHORED_BOOK`] anchored on an HTTP snapshot, refuses a book id its caps do not
//! declare with nothing sent, reports a gap on the channel that broke, and declares a keepalive;
//! and (FBC-ja3) its URLs from the configuration with the credential spans it marks
//! (`url.rs`), a resync over REST retried until it is decoded whole ([`ToyExec::rest_resync`])
//! and a ping on its order-entry connection ([`ToyExec::pinging`]), both as its factory builds
//! it. Trades, funding, mark, index and stats arrive with FBC-z2s.
//!
//! Protocol: its own, describing no real venue, as `fbc-core`'s toy: one record per line,
//! `kind|key=value|...`. A request's first record names its `rpc`; a batch's first record
//! counts its items (`n`), one record per item following, each numbered (`i`). Prices are
//! ticks, sizes lots, `ts` the signature time in nanoseconds from the encode context. What the
//! venue sends is one record per frame; its fields are listed where it is decoded.

mod decode;
mod exec;
mod factory;
mod md;
mod session;
mod signer;
mod url;

use core::num::NonZeroU32;
use core::time::Duration;

use fbc_core::{
    AckModel, AmendAck, AmendCaps, AmendQty, AssetSym, Batch, BookCaps, BookId, Cadence,
    CancelBatch, CancelOnDisconnect, Channel, Charset, ClientIdFormat, ConnTopology, Continuity,
    DecodeScope, Encoding, ExecCaps, Feature, FeedSource, FillCaps, FillSource, FundingCaps,
    FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec, LimitScope, Lots, MatchingCaps,
    MdCaps, Namespace, NonceScope, OpKind, OrderCaps, OrderKindTag, OrderingKey, PriceGrid,
    QueueModelQuality, RateLimit, Readiness, RefKind, SizeStep, SnapshotSource, SpecTable,
    StpScope, StreamId, Support, TagSet, TifTag, TimerTag, TradeCaps, TradingStatus, UnderlyingId,
    VenueCaps, VenueFeeSign, VenueId, WallNs, dispatch,
};
use rust_decimal::Decimal;

pub use decode::{REJECT_CODES, reject_kind};
pub use exec::ToyExec;
pub use factory::{
    EXEC_URL_KEY, EXEC_URL_REDACT_KEY, MD_STREAM, MD_URL_KEY, MD_URL_REDACT_KEY, REST_URL_KEY,
    REST_URL_REDACT_KEY, ToyFactory,
};
pub use md::ToyMd;
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
/// The token the toy authenticates with: synthetic, of no account and no venue. It stands for
/// a session credential, so it goes out in a redaction span and `redact_inbound` names it in
/// the acknowledgement that echoes it (decision 0028).
pub const TOY_TOKEN: &str = "toy-session-token";

/// The book channel whose snapshot comes in one frame.
pub const BOOK: BookId = BookId(0);
/// The book channel anchored on an HTTP snapshot (`BookCaps::rest_anchor`).
pub const ANCHORED_BOOK: BookId = BookId(1);
/// How often the market-data codec's keepalive frame goes out.
pub const KEEPALIVE_EVERY: Duration = Duration::from_secs(20);
/// How long an anchor's request may take.
pub const ANCHOR_TIMEOUT: Duration = Duration::from_secs(2);
/// How long after a failed anchor it is asked for again.
pub const ANCHOR_RETRY: Duration = Duration::from_secs(1);
/// The most deltas an anchored channel holds while its anchor is asked for; one more asks again.
pub const MAX_HELD: usize = 64;
/// The configuration key of the anchors' base URL (`http://` or `https://`): an anchor is
/// asked for at `<base>/book/<sym>`.
pub const ANCHOR_URL_KEY: &str = "toy.md.anchor_url";
/// The configuration key of the credential spans in [`ANCHOR_URL_KEY`]'s URL (`url.rs`).
pub const ANCHOR_URL_REDACT_KEY: &str = "toy.md.anchor_url.redact";

/// How often a pinging order-entry codec sends its ping ([`ToyExec::pinging`]).
pub const PING_EVERY: Duration = Duration::from_secs(15);
/// The timer a pinging order-entry codec sends its ping on.
pub const PING_TAG: TimerTag = TimerTag(1);
/// How long a resync over REST may take ([`ToyExec::rest_resync`]).
pub const RESYNC_TIMEOUT: Duration = Duration::from_secs(2);
/// How long after a resync over REST that failed, or came back unreadable, it is asked again.
pub const RESYNC_RETRY: Duration = Duration::from_secs(1);
/// The timer a resync over REST is asked again on.
pub const RESYNC_RETRY_TAG: TimerTag = TimerTag(2);

/// Whether the toy's fills carry a venue fill id ([`FillCaps::fill_id`]). Venues differ here,
/// and the flag is one per venue, so the toy is declared either way and its frames keep to the
/// declaration it was built with (Codex r4172835753).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FillIds {
    /// Every fill names its venue fill id; [`caps`] declares this.
    Venue,
    /// No fill names one: fills are keyed by order and cumulative quantity.
    Derived,
}

/// What the toy declares, its fills carrying venue fill ids. Of market data it declares its two
/// book channels so far; FBC-z2s adds the other feeds.
pub fn caps() -> VenueCaps {
    caps_for(FillIds::Venue)
}

/// What the toy declares with fill ids as `fill_ids` says; nothing else differs.
pub fn caps_for(fill_ids: FillIds) -> VenueCaps {
    let by_venue_or_nonce = TagSet::of(&[RefKind::Venue, RefKind::PlacementNonce]);
    VenueCaps {
        exec: Some(ExecCaps {
            order: OrderCaps {
                kinds: TagSet::of(&[OrderKindTag::Limit]),
                tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc]),
                channels: TagSet::of(&[Channel::Public]),
                post_only: true,
                reduce_only: true,
                // A post-only order that is also immediate-or-cancel is refused: it could
                // neither rest nor take.
                flag_conflicts: vec![(Feature::PostOnly, Feature::Ioc)],
                // An amend names its order by the venue's id only, and its quantity is what is
                // left to fill.
                amend: Some(AmendCaps {
                    refs: TagSet::of(&[RefKind::Venue]),
                    price: true,
                    qty: true,
                    flags: true,
                    when_partially_filled: true,
                    reject_keeps_original: true,
                    // An amended order gets a new venue id, which the order event reporting
                    // the replaced order names.
                    keeps_venue_id: false,
                    ack: AmendAck::ReplacedEvent,
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
                // Protection per connection, requested again on each (FBC-w19): the order-entry
                // session arms it after every epoch's authentication.
                cancel_on_disconnect: CancelOnDisconnect::PerConnection {
                    rearm_on_reconnect: true,
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
                realized_pnl: true,
                realized_funding: true,
                fee_sign: VenueFeeSign::PositiveIsCost,
                fee_asset_reported: true,
                fill_id: fill_ids == FillIds::Venue,
                replays_fills_on_reconnect: true,
            },
        }),
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            encoding: Encoding::Text,
            touch_sources: Vec::new(),
            books: vec![book_caps("book", false), book_caps("rpi_book", true)],
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
        // One account budget counts every request the codecs write (decision 0018), market
        // data's subscriptions and anchors included (Codex r4216139636).
        limits: vec![RateLimit {
            scope: LimitScope::Account,
            ops: TagSet::of(&[
                OpKind::Place,
                OpKind::Amend,
                OpKind::Cancel,
                OpKind::CancelAll,
                OpKind::Query,
                OpKind::Subscribe,
                OpKind::Rest,
                OpKind::Control,
            ]),
            per: Duration::from_secs(1),
            units: 50,
        }],
        readiness_ceiling: Readiness::Record,
    }
}

/// One of the toy's book channels: realtime, sequenced plus one, unwindowed; `anchored` is
/// [`ANCHORED_BOOK`], which also shows RPI liquidity.
fn book_caps(channel: &'static str, anchored: bool) -> BookCaps {
    let channels: &[Channel] = match anchored {
        true => &[Channel::Public, Channel::Rpi],
        false => &[Channel::Public],
    };
    BookCaps {
        channel,
        max_depth: 50,
        cadence: Cadence::Realtime,
        continuity: Continuity::PlusOne,
        windowed: false,
        rest_anchor: anchored,
        includes_channels: TagSet::of(channels),
        queue_model: QueueModelQuality::BracketOnly,
    }
}

/// Runs `f` in the decode scope the core lends for the toy's caps and namespace.
pub fn with_scope<R>(f: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R) -> R {
    with_scope_for(FillIds::Venue, f)
}

/// Runs `f` in the decode scope the core lends for the toy declared with `fill_ids`.
pub fn with_scope_for<R>(fill_ids: FillIds, f: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R) -> R {
    dispatch(&caps_for(fill_ids), OWN_NS, f)
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
