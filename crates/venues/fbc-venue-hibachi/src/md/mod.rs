//! The market-data codec for Hibachi's anonymous `/ws/market` WebSocket (decisions 0002, 0014,
//! 0124 and 0129): one connection carrying every subscribed instrument's `orderbook`,
//! `ask_bid_price`, `trades`, `mark_price`, `spot_price` and `funding_rate_estimation`.
//!
//! # Subscriptions
//!
//! Each subscription is one frame with the `parameters` key (the Java library's
//! `HibachiMarketSubscribeMessage.java` L13-L19, which notes that key,
//! <https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/commons/hibachi-common-api/src/main/java/com/fueledbychai/hibachi/common/api/ws/HibachiMarketSubscribeMessage.java#L13-L19>;
//! SDK `api_ws_market.py`), naming one instrument and one topic, charged one subscription of
//! that instrument (decision 0018):
//! `{"method":"subscribe","parameters":{"subscriptions":[{"symbol":..,"topic":..}]}}`, and an
//! `unsubscribe` the same way. An `orderbook` subscription also names its `granularity`,
//! `quote_grid.unwrap_or(tick)` of the instrument's spec as written, and its `depth`, the
//! configured [`KEY_ORDERBOOK_DEPTH`](crate::KEY_ORDERBOOK_DEPTH): Hibachi's WebSocket market
//! docs take both as an orderbook subscription's parameters, one set per subscription, and no
//! source shows them in a frame, so they are written beside the symbol and topic and each frame's
//! own `granularity` and `depth` confirm them. Hibachi's market-maker-partner book topic is
//! neither subscribed nor decoded.
//!
//! # What is decoded
//!
//! - `orderbook` ([`book`]): a Snapshot replaces the book and an Update applies each side's
//!   window and levels; every frame is applied to the codec's own copy and checked before any
//!   of its events is pushed, and a frame that shows the book may be wrong gaps it, after which
//!   the codec resubscribes that instrument's orderbook and, if no Snapshot answers, asks for a
//!   reconnect (decision 0129).
//! - `ask_bid_price`: a [`MdEvent::Touch`] only from a frame carrying each side's price and size
//!   (`bidPrice`, `bidSize`, `askPrice`, `askSize`; the sizes as the Java library reads them,
//!   `HibachiQuoteEngine.java` L548-L575), the bid below the ask. Any other frame is counted, and
//!   while a touch is in force it is invalidated with `Health { feed: Feed::Touch(..), h: Gap }`;
//!   the next usable frame's Touch restores it. No size is ever synthesized.
//! - `trades`: `{"trade":{..}}` or `{"trades":[..]}`, each with `price`, `quantity`, `takerSide`
//!   and, where present, `timestamp` in whole seconds; no trade id. Where `takerSide` is absent
//!   its alias `side` is read, as the Java library reads it (`HibachiQuoteEngine.java` L865 at
//!   b4188a5, linked from `caps.rs`); either must be `Buy` or `Sell`, the SDK's `TakerSide`, and
//!   any other value is refused, never taken for a side.
//! - `mark_price`'s `markPrice`, or its alias `price` where `markPrice` is absent (the Java
//!   library, `HibachiQuoteEngine.java` L581), as [`MdEvent::Mark`], and `spot_price`'s
//!   `spotPrice` as [`MdEvent::Index`], exact. No other alias the Java library reads (a trade's
//!   `size` for `quantity`) is read.
//! - `funding_rate_estimation`: `estimatedFundingRate`, flat or under `fundingRateEstimation`, an
//!   eight-hour rate ([`FUNDING_ESTIMATE_PERIOD`](crate::FUNDING_ESTIMATE_PERIOD)), as
//!   [`MdEvent::Funding`] per the one-hour settlement interval: its eighth, in units of 1e-12,
//!   its `interval` [`FUNDING_SETTLEMENT_INTERVAL`] (the owner's conclusion of 2026-10-28,
//!   FBC-204c, decision 0131; the crate docs' Funding section). Nothing is rounded: the
//!   estimate's eighth must be a whole number of 1e-12, and an estimate whose eighth is finer,
//!   one finer than 1e-12 itself included, is refused as malformed, so every pushed rate is
//!   exactly an eighth of what the frame states. An estimate stated to nine decimal places or
//!   fewer always divides (1e-9 is 1000 units of 1e-12), and Hibachi's example frame states six
//!   (`-0.000040`). `next` is `None`: the frame's `nextFundingTimestamp` is not read until a
//!   fixture shaped after evidence pins its unit.
//!
//! No exchange timestamp is synthesized: an event carries one only from its frame's root
//! `timestamp_ms` (epoch milliseconds) or its trade's `timestamp` (epoch seconds), and none of
//! Hibachi's example frames carries the former. A frame with no `topic` is an acknowledgement
//! (`{"id":..,"status":200}`, the docs' answer to an unsubscribe) or is refused. A frame of a
//! topic not subscribed (one still in flight after an unsubscribe) is passed over.
//!
//! What the codec counts ([`HibachiMdCounts`]) is read on the concrete [`HibachiMd`]: the
//! `MdCodec` interface has no counter of its own beyond refused frames. So is its own copy of each
//! orderbook ([`HibachiMd::book_copy`]), which a book built from its events equals.

mod book;

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{
    Aggressor, ConfigError, DecodeError, DecodeScope, Effect, Effects, ExchNs, ExchTsKind, Feed,
    FeedHealth, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId,
    InstrumentSpec, Keepalive, Lots, Lvl, MdCodec, MdEvent, MdSink, MonoNs, OpKind, PxExact,
    RateCharge, RawFrame, SpecTable, StreamId, Subscription, TimerTag, TrafficClass, VenueConfig,
    VenueError, VenueMeta, WallNs, WireSlice,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value, json};

use self::book::{BookParams, Broken, OrderBook, Phase};
use crate::config::Settings;
use crate::discover::plain_decimal;
use crate::{BOOK_ORDERBOOK, ESTIMATE_INTERVALS, FUNDING_SETTLEMENT_INTERVAL, TOUCH_ASK_BID_PRICE};

/// Nanoseconds in a second and in a millisecond.
const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

/// The topic a feed is subscribed under (Hibachi's WebSocket market docs), or `None` for a feed
/// this codec does not decode.
pub(crate) fn topic(feed: Feed) -> Option<&'static str> {
    match feed {
        Feed::Book(BOOK_ORDERBOOK) => Some("orderbook"),
        Feed::Touch(TOUCH_ASK_BID_PRICE) => Some("ask_bid_price"),
        Feed::Trades => Some("trades"),
        Feed::Mark => Some("mark_price"),
        Feed::Index => Some("spot_price"),
        Feed::Funding => Some("funding_rate_estimation"),
        Feed::Book(_) | Feed::Touch(_) | Feed::Stats => None,
    }
}

/// What a frame's topic carries: a topic never subscribed (`klines`, and any other) is none.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Topic {
    Orderbook,
    AskBidPrice,
    Trades,
    MarkPrice,
    SpotPrice,
    FundingRateEstimation,
}

impl Topic {
    fn read(topic: &str) -> Option<Topic> {
        match topic {
            "orderbook" => Some(Topic::Orderbook),
            "ask_bid_price" => Some(Topic::AskBidPrice),
            "trades" => Some(Topic::Trades),
            "mark_price" => Some(Topic::MarkPrice),
            "spot_price" => Some(Topic::SpotPrice),
            "funding_rate_estimation" => Some(Topic::FundingRateEstimation),
            _ => None,
        }
    }

    fn feed(self) -> Feed {
        match self {
            Topic::Orderbook => Feed::Book(BOOK_ORDERBOOK),
            Topic::AskBidPrice => Feed::Touch(TOUCH_ASK_BID_PRICE),
            Topic::Trades => Feed::Trades,
            Topic::MarkPrice => Feed::Mark,
            Topic::SpotPrice => Feed::Index,
            Topic::FundingRateEstimation => Feed::Funding,
        }
    }
}

/// Whether `sub` can be subscribed: a feed the codec decodes, on an instrument `specs` spells.
pub(crate) fn plannable(
    settings: &Settings,
    specs: &SpecTable,
    sub: Subscription,
) -> Result<(), VenueError> {
    check(settings, specs, sub).map(|_| ())
}

/// `sub` checked: its topic, its instrument's spec, and an orderbook's parameters.
fn check<'a>(
    settings: &Settings,
    specs: &'a SpecTable,
    sub: Subscription,
) -> Result<(&'static str, &'a InstrumentSpec, Option<BookParams>), VenueError> {
    let topic = topic(sub.feed).ok_or(VenueError::UnsupportedFeed(sub))?;
    let spec = specs
        .get(sub.inst)
        .ok_or(VenueError::UnknownInstrument(sub.inst))?;
    let params = (sub.feed == Feed::Book(BOOK_ORDERBOOK)).then(|| {
        // The quote grid exactly as the contract lists it, or the tick (the owner's decision of
        // 2026-10-25 on FBC-jyyl).
        let granularity = spec.quote_grid.unwrap_or_else(|| spec.price_grid.finest());
        BookParams {
            granularity_text: granularity.to_string(),
            granularity,
            depth: settings.depth,
        }
    });
    Ok((topic, spec, params))
}

/// What the codec counted. Each is a frame, or a side of one, it passed over or found wrong,
/// or an action it took on its own.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct HibachiMdCounts {
    /// Orderbook sides sent as null, each cleared (decision 0129).
    pub null_sides: u64,
    /// Orderbook frames not as documented (an absent side, a side not null lacking either bound,
    /// a level or bound unreadable or off the tick), each gapping its book.
    pub malformed_books: u64,
    /// Orderbook frames whose `granularity` or `depth` differs from the subscription's or is
    /// missing, each gapping its book.
    pub param_mismatches: u64,
    /// Orderbook frames with a level off the subscription's granularity, each gapping its book.
    pub off_granularity: u64,
    /// Snapshots with no level on either side, each gapping its book.
    pub empty_snapshots: u64,
    /// Orderbook frames failing the bounds check (a crossed book included), each gapping its
    /// book.
    pub bounds_failures: u64,
    /// Orderbook frames failing it only by a side thinner than the depth, counted apart, each
    /// gapping its book.
    pub thin_sides: u64,
    /// Orderbook frames leaving the best bid equal to the best ask: not crossed, so taken, and
    /// counted apart.
    pub locked_books: u64,
    /// Updates for a book with no valid copy (before a Snapshot, or once gapped), passed over.
    pub updates_without_book: u64,
    /// Resubscribes the codec made of a broken orderbook.
    pub resubscribes: u64,
    /// Reconnects it asked for when a resubscribe brought no Snapshot.
    pub reconnects: u64,
    /// `ask_bid_price` frames missing a side's price or size: no Touch.
    pub unsized_touches: u64,
    /// `ask_bid_price` frames with the bid at or above the ask: no Touch.
    pub crossed_touches: u64,
    /// `ask_bid_price` frames with a price or size unreadable, off its grid, or a size of zero:
    /// no Touch.
    pub malformed_touches: u64,
    /// Acknowledgements (`{"id":..,"status":200}`).
    pub acks: u64,
    /// Frames of a topic not subscribed on this connection, passed over.
    pub unsubscribed_frames: u64,
}

/// The codec for one connection epoch of the `/ws/market` endpoint.
pub struct HibachiMd {
    settings: Result<Settings, ConfigError>,
    /// The subscriptions taken on this connection.
    active: BTreeSet<Subscription>,
    /// Each subscribed orderbook.
    books: BTreeMap<InstrumentId, OrderBook>,
    /// The instruments whose `ask_bid_price` Touch is in force: one pushed and not invalidated
    /// since. Kept when the subscription is removed, since nothing is pushed then, so that a
    /// subscription added again invalidates that Touch on its first unusable frame.
    touches: BTreeSet<InstrumentId>,
    /// What every book's frames and timers reach besides the book.
    shared: Shared,
}

/// What a book's frames and timers reach besides the book itself.
struct Shared {
    stream: StreamId,
    /// The last snapshot epoch given each instrument's book.
    epochs: BTreeMap<InstrumentId, u32>,
    /// The last timer tag given.
    last_tag: u64,
    /// Whether this epoch asked for a reconnect: once is enough.
    reconnecting: bool,
    counts: HibachiMdCounts,
}

impl HibachiMd {
    /// A codec writing to `stream` under `cfg`. A configuration the factory would refuse gives a
    /// codec that refuses every subscription with the same error.
    pub fn new(stream: StreamId, cfg: &VenueConfig) -> HibachiMd {
        HibachiMd {
            settings: Settings::read(cfg),
            active: BTreeSet::new(),
            books: BTreeMap::new(),
            touches: BTreeSet::new(),
            shared: Shared {
                stream,
                epochs: BTreeMap::new(),
                last_tag: 0,
                reconnecting: false,
                counts: HibachiMdCounts::default(),
            },
        }
    }

    /// What it counted so far.
    pub fn counts(&self) -> HibachiMdCounts {
        self.shared.counts
    }

    /// Its own copy of `inst`'s orderbook, the bids and the asks each best first: the levels the
    /// last frame it took left, which passed the bounds check. A book built from the events it
    /// pushed holds the same levels. `None` while it holds no valid copy: before a Snapshot, from
    /// a gap until a Snapshot passes, or with no orderbook subscription of `inst`.
    pub fn book_copy(&self, inst: InstrumentId) -> Option<(Vec<Lvl>, Vec<Lvl>)> {
        let book = self.books.get(&inst)?;
        (book.phase == Phase::Valid).then(|| book.levels.best_first())
    }

    /// `frame` has no topic: an acknowledgement, or refused.
    fn on_control(&mut self, frame: &Map<String, Value>) -> Result<(), DecodeError> {
        if frame.get("status").and_then(Value::as_u64) == Some(200) {
            self.shared.counts.acks += 1;
            return Ok(());
        }
        Err(DecodeError::Malformed(
            "a frame with no topic that is not an acknowledgement",
        ))
    }

    /// An `ask_bid_price` frame for `inst`.
    fn on_touch(
        &mut self,
        frame: &Map<String, Value>,
        spec: &InstrumentSpec,
        sink: &mut dyn MdSink,
    ) {
        let inst = spec.id;
        let meta = root_meta(frame);
        let touch = meta
            .as_ref()
            .map_err(|_| TouchFault::Malformed)
            .and_then(|_| {
                let data = frame.get("data").and_then(Value::as_object);
                touch(data.ok_or(TouchFault::Malformed)?, spec)
            });
        let meta = meta.unwrap_or(VenueMeta::NONE);
        match touch {
            Ok((bid, ask)) => {
                self.touches.insert(inst);
                let (bid, ask) = (Some(bid), Some(ask));
                let source = TOUCH_ASK_BID_PRICE;
                sink.push(
                    meta,
                    MdEvent::Touch {
                        inst,
                        bid,
                        ask,
                        source,
                    },
                );
            }
            Err(fault) => {
                let counts = &mut self.shared.counts;
                let counter = match fault {
                    TouchFault::Unsized => &mut counts.unsized_touches,
                    TouchFault::Crossed => &mut counts.crossed_touches,
                    TouchFault::Malformed => &mut counts.malformed_touches,
                };
                *counter += 1;
                if self.touches.remove(&inst) {
                    let feed = Feed::Touch(TOUCH_ASK_BID_PRICE);
                    let h = FeedHealth::Gap;
                    sink.push(meta, MdEvent::Health { inst, feed, h });
                }
            }
        }
    }
}

impl Shared {
    /// Asks to write `method` for `symbol`'s `topic` (with an orderbook's parameters), charged
    /// one subscription of `inst`.
    fn send(
        &self,
        method: &str,
        inst: InstrumentId,
        symbol: &str,
        topic: &str,
        params: Option<&BookParams>,
        fx: &mut Effects,
    ) {
        // Keys in alphabetical order, as Hibachi's examples write them.
        let subscription = match params {
            Some(p) => json!({
                "depth": p.depth,
                "granularity": p.granularity_text,
                "symbol": symbol,
                "topic": topic,
            }),
            None => json!({"symbol": symbol, "topic": topic}),
        };
        let frame = json!({
            "method": method,
            "parameters": {"subscriptions": [subscription]},
        });
        fx.push(Effect::Send {
            stream: self.stream,
            frame: WireSlice::plain(frame.to_string().into_bytes()),
            rpc: None,
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Subscribe, Some(inst)),
        });
    }

    /// A fresh timer, `after` from now.
    fn arm(&mut self, after: Duration, fx: &mut Effects) -> TimerTag {
        self.last_tag += 1;
        let tag = TimerTag(self.last_tag);
        fx.push(Effect::Timer { tag, after });
        tag
    }

    /// Unsubscribes and subscribes `book` again with the same granularity and depth, and waits
    /// for its Snapshot.
    fn resubscribe(&mut self, book: &mut OrderBook, fx: &mut Effects) {
        book.forget();
        let (inst, symbol) = (book.inst, book.symbol.as_str());
        self.send("unsubscribe", inst, symbol, "orderbook", None, fx);
        self.send(
            "subscribe",
            inst,
            symbol,
            "orderbook",
            Some(&book.params),
            fx,
        );
        self.counts.resubscribes += 1;
        let timer = self.arm(book.interval, fx);
        book.phase = Phase::Awaiting {
            resubscribed: true,
            timer,
        };
    }

    /// An orderbook frame for `book`, `spec`'s.
    fn on_book(
        &mut self,
        book: &mut OrderBook,
        frame: &Map<String, Value>,
        spec: &InstrumentSpec,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        let inst = spec.id;
        let kind = frame
            .get("messageType")
            .and_then(Value::as_str)
            .unwrap_or("");
        if kind == "Update" && book.phase != Phase::Valid {
            self.counts.updates_without_book += 1;
            return;
        }
        let epoch = self.epochs.entry(inst).or_insert(0);
        let meta = root_meta(frame);
        let applied = match meta {
            Ok(_) => book.apply(kind, frame, spec, epoch),
            Err(_) => Err(Broken::Malformed),
        };
        let meta = meta.unwrap_or(VenueMeta::NONE);
        match applied {
            Ok(applied) => {
                book.phase = Phase::Valid;
                self.counts.null_sides += applied.nulls;
                self.counts.locked_books += u64::from(applied.locked);
                for event in applied.events {
                    sink.push(meta, event);
                }
            }
            Err(broken) => {
                let counter = match broken {
                    Broken::Malformed => &mut self.counts.malformed_books,
                    Broken::Params => &mut self.counts.param_mismatches,
                    Broken::OffGranularity => &mut self.counts.off_granularity,
                    Broken::Empty => &mut self.counts.empty_snapshots,
                    Broken::Bounds => &mut self.counts.bounds_failures,
                    Broken::Thin => &mut self.counts.thin_sides,
                };
                *counter += 1;
                book.forget();
                sink.push(
                    meta,
                    MdEvent::Health {
                        inst,
                        feed: Feed::Book(BOOK_ORDERBOOK),
                        h: FeedHealth::Gap,
                    },
                );
                // Already waiting for the retry, or for the reconnect asked for: that stands. So
                // does a wait for a Snapshot, unless this frame is the Snapshot that came back
                // failing, so that no other frame keeps the reconnect from being asked for.
                // Otherwise the retry interval starts now.
                let stands = match book.phase {
                    Phase::Gapped { .. } | Phase::Reconnecting => true,
                    Phase::Awaiting { .. } => kind != "Snapshot",
                    Phase::Valid => false,
                };
                if !stands {
                    let timer = self.arm(book.interval, fx);
                    book.phase = Phase::Gapped { timer };
                }
            }
        }
    }

    /// `book`'s timer fired: waiting for its first Snapshot, or gapped, it is resubscribed (a gap
    /// reported for the first); waiting for the Snapshot a resubscribe asked for, it asks for one
    /// reconnect.
    fn on_book_timer(&mut self, book: &mut OrderBook, sink: &mut dyn MdSink, fx: &mut Effects) {
        match book.phase {
            Phase::Awaiting {
                resubscribed: true, ..
            } => {
                book.phase = Phase::Reconnecting;
                if !self.reconnecting {
                    self.reconnecting = true;
                    self.counts.reconnects += 1;
                    fx.push(Effect::Reconnect {
                        stream: self.stream,
                        reason: "no Hibachi orderbook Snapshot answered a resubscribe",
                    });
                }
            }
            phase => {
                if matches!(phase, Phase::Awaiting { .. }) {
                    let (inst, feed, h) = (book.inst, Feed::Book(BOOK_ORDERBOOK), FeedHealth::Gap);
                    sink.push(VenueMeta::NONE, MdEvent::Health { inst, feed, h });
                }
                self.resubscribe(book, fx);
            }
        }
    }
}

/// Why an `ask_bid_price` frame gives no Touch.
enum TouchFault {
    /// A side's price or size is missing.
    Unsized,
    /// The bid is at or above the ask.
    Crossed,
    /// A value is unreadable, off its grid, or a size is zero.
    Malformed,
}

/// The two sides of an `ask_bid_price` frame's data, each with its price and size.
fn touch(data: &Map<String, Value>, spec: &InstrumentSpec) -> Result<(Lvl, Lvl), TouchFault> {
    let field = |key: &str| match data.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.as_str())),
        Some(_) => Err(TouchFault::Malformed),
    };
    let side = |px: &str, qty: &str| -> Result<Option<Lvl>, TouchFault> {
        let (Some(px), Some(qty)) = (field(px)?, field(qty)?) else {
            return Ok(None);
        };
        let px = plain_decimal(px).and_then(|px| book::ticks(px, spec));
        let qty = book::lots(qty, spec);
        match (px, qty) {
            (Some(px), Some(qty)) if qty != Lots::ZERO => Ok(Some(Lvl { px, qty })),
            _ => Err(TouchFault::Malformed),
        }
    };
    let bid = side("bidPrice", "bidSize")?;
    let ask = side("askPrice", "askSize")?;
    let (Some(bid), Some(ask)) = (bid, ask) else {
        return Err(TouchFault::Unsized);
    };
    if bid.px >= ask.px {
        return Err(TouchFault::Crossed);
    }
    Ok((bid, ask))
}

/// What the frame says of its own time: its root `timestamp_ms`, epoch milliseconds, where it
/// carries one; none of Hibachi's example frames does. Never synthesized.
fn root_meta(frame: &Map<String, Value>) -> Result<VenueMeta, DecodeError> {
    let ts = match frame.get("timestamp_ms") {
        None => None,
        Some(ms) => {
            let ns = ms.as_i64().and_then(|ms| ms.checked_mul(NANOS_PER_MILLI));
            Some(
                ns.filter(|ns| *ns >= 0)
                    .ok_or(DecodeError::Malformed("timestamp_ms"))?,
            )
        }
    };
    Ok(meta(ts))
}

fn meta(ts: Option<i64>) -> VenueMeta {
    VenueMeta {
        exch_ts: ts.map(ExchNs),
        // No source says which instant Hibachi's timestamps mark.
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: None,
    }
}

/// A frame's `data` object.
fn data(frame: &Map<String, Value>) -> Result<&Map<String, Value>, DecodeError> {
    let data = frame.get("data").and_then(Value::as_object);
    data.ok_or(DecodeError::Malformed("data"))
}

/// An exact price under `key`, or under `alias` where `key` is absent: a plain decimal string.
fn exact(
    data: &Map<String, Value>,
    key: &'static str,
    alias: Option<&str>,
) -> Result<PxExact, DecodeError> {
    let px = data
        .get(key)
        .or_else(|| alias.and_then(|alias| data.get(alias)))
        .and_then(Value::as_str)
        .and_then(plain_decimal);
    px.and_then(PxExact::from_decimal)
        .ok_or(DecodeError::Malformed(key))
}

/// The trades of a `trades` frame's data, each with its time: `{"trade":{..}}` or
/// `{"trades":[..]}`.
fn trades(
    data: &Map<String, Value>,
    spec: &InstrumentSpec,
    root: VenueMeta,
) -> Result<Vec<(VenueMeta, MdEvent)>, DecodeError> {
    let listed: Vec<&Value> = match (data.get("trade"), data.get("trades")) {
        (Some(one @ Value::Object(_)), None) => vec![one],
        (None, Some(Value::Array(many))) => many.iter().collect(),
        _ => return Err(DecodeError::Malformed("trade or trades")),
    };
    listed
        .into_iter()
        .map(|trade| {
            let trade = trade.as_object().ok_or(DecodeError::Malformed("trade"))?;
            let px = trade.get("price").and_then(Value::as_str);
            let px = px
                .and_then(plain_decimal)
                .and_then(|px| book::ticks(px, spec));
            let px = px.ok_or(DecodeError::Malformed("price"))?;
            let qty = trade.get("quantity").and_then(Value::as_str);
            let qty = qty
                .and_then(|q| book::lots(q, spec))
                .filter(|q| *q != Lots::ZERO);
            let qty = qty.ok_or(DecodeError::Malformed("quantity"))?;
            // `side` only where `takerSide` is absent (the Java library's alias).
            let side = trade.get("takerSide").or_else(|| trade.get("side"));
            let aggressor = match side.and_then(Value::as_str) {
                Some("Buy") => Aggressor::Buyer,
                Some("Sell") => Aggressor::Seller,
                _ => return Err(DecodeError::Malformed("takerSide")),
            };
            let meta = match trade.get("timestamp") {
                None => root,
                Some(secs) => {
                    let ns = secs.as_i64().and_then(|s| s.checked_mul(NANOS_PER_SEC));
                    let ns = ns.filter(|ns| *ns >= 0);
                    meta(Some(ns.ok_or(DecodeError::Malformed("timestamp"))?))
                }
            };
            let inst = spec.id;
            let id = None;
            Ok((
                meta,
                MdEvent::Trade {
                    inst,
                    id,
                    aggressor,
                    px,
                    qty,
                },
            ))
        })
        .collect()
}

/// A `funding_rate_estimation` frame's rate per settlement interval, in units of 1e-12: the
/// eight-hour estimate, flat (`estimatedFundingRate`) or nested
/// (`fundingRateEstimation.estimatedFundingRate`), one of the two, divided by
/// [`ESTIMATE_INTERVALS`] exactly. Malformed where that is not a whole number of 1e-12: never
/// rounded.
fn funding_rate(data: &Map<String, Value>) -> Result<i64, DecodeError> {
    let bad = DecodeError::Malformed("estimatedFundingRate");
    let rate = match (
        data.get("estimatedFundingRate"),
        data.get("fundingRateEstimation"),
    ) {
        (Some(rate), None) => rate,
        (None, Some(Value::Object(nested))) => nested.get("estimatedFundingRate").ok_or(bad)?,
        _ => return Err(bad),
    };
    let text = rate.as_str().ok_or(bad)?;
    let (negative, digits) = match text.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, text),
    };
    let magnitude = plain_decimal(digits).ok_or(bad)?;
    let e12 = magnitude
        .checked_mul(Decimal::from(1_000_000_000_000_i64))
        .ok_or(bad)?;
    // The estimate in units of 1e-12, exactly, then its share of one interval, exactly.
    let e12 = e12
        .fract()
        .is_zero()
        .then(|| e12.to_i64())
        .flatten()
        .ok_or(bad)?;
    let intervals = ESTIMATE_INTERVALS as i64;
    let per_interval = (e12 % intervals == 0).then_some(e12 / intervals);
    let per_interval = per_interval.ok_or(bad)?;
    Ok(if negative {
        -per_interval
    } else {
        per_interval
    })
}

impl MdCodec for HibachiMd {
    /// Nothing to send: the subscriptions follow, and the feed takes no authentication.
    fn on_open(&mut self, _fx: &mut Effects) {}

    /// One `unsubscribe` frame per subscription removed, then one `subscribe` frame per
    /// subscription added, each charged one subscription of its instrument; an orderbook added
    /// waits for its Snapshot for the configured interval. Every subscription is checked before
    /// anything is asked for, so a refusal sends nothing. A subscription already taken, or one
    /// not taken removed, sends nothing.
    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        let settings = self
            .settings
            .as_ref()
            .map_err(|err| VenueError::Config(*err))?;
        let checked = |subs: &[Subscription]| -> Result<Vec<_>, VenueError> {
            subs.iter()
                .map(|&sub| check(settings, specs, sub).map(|c| (sub, c)))
                .collect()
        };
        let removes = checked(remove)?;
        let adds = checked(add)?;
        for (sub, (topic, spec, _)) in removes {
            if self.active.remove(&sub) {
                let symbol = spec.venue_symbol.as_wire();
                self.shared
                    .send("unsubscribe", sub.inst, symbol, topic, None, fx);
                // A book's state goes with it, so a timer it armed finds nothing. A Touch in
                // force stays recorded: removing pushes nothing that invalidates it.
                if let Feed::Book(_) = sub.feed {
                    self.books.remove(&sub.inst);
                }
            }
        }
        for (sub, (topic, spec, params)) in adds {
            if !self.active.insert(sub) {
                continue;
            }
            let symbol = spec.venue_symbol.as_wire();
            let shared = &mut self.shared;
            shared.send("subscribe", sub.inst, symbol, topic, params.as_ref(), fx);
            if let Some(params) = params {
                let timer = shared.arm(settings.resubscribe, fx);
                let book = OrderBook {
                    inst: sub.inst,
                    symbol: symbol.to_owned(),
                    params,
                    interval: settings.resubscribe,
                    phase: Phase::Awaiting {
                        resubscribed: false,
                        timer,
                    },
                    levels: Default::default(),
                };
                self.books.insert(sub.inst, book);
            }
        }
        Ok(())
    }

    /// One frame. An orderbook or `ask_bid_price` frame of a subscribed instrument is never
    /// refused: what is wrong with it is counted and reported as the feed's health. Any other
    /// frame is decoded whole before anything is pushed.
    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let RawFrame::Text(text) = f else {
            return Err(DecodeError::Malformed("a binary frame"));
        };
        let value: Value =
            serde_json::from_str(text).map_err(|_| DecodeError::Malformed("not JSON"))?;
        let Value::Object(frame) = value else {
            return Err(DecodeError::Malformed("not a JSON object"));
        };
        let Some(topic) = frame.get("topic") else {
            return self.on_control(&frame);
        };
        let topic = topic.as_str().ok_or(DecodeError::Malformed("topic"))?;
        let topic = Topic::read(topic).ok_or(DecodeError::Malformed("a topic never subscribed"))?;
        let symbol = frame.get("symbol").and_then(Value::as_str);
        let symbol = symbol.ok_or(DecodeError::Malformed("symbol"))?;
        let spec = specs
            .by_symbol(symbol)
            .ok_or(DecodeError::UnknownInstrument)?;
        let inst = spec.id;
        let feed = topic.feed();
        let book = self.books.get_mut(&inst);
        let subscribed = match topic {
            Topic::Orderbook => book.is_some(),
            _ => self.active.contains(&Subscription { inst, feed }),
        };
        if !subscribed {
            self.shared.counts.unsubscribed_frames += 1;
            return Ok(());
        }
        match topic {
            Topic::Orderbook => {
                if let Some(book) = book {
                    self.shared.on_book(book, &frame, spec, sink, fx);
                }
            }
            Topic::AskBidPrice => self.on_touch(&frame, spec, sink),
            Topic::Trades => {
                let root = root_meta(&frame)?;
                for (meta, trade) in trades(data(&frame)?, spec, root)? {
                    sink.push(meta, trade);
                }
            }
            Topic::MarkPrice => {
                let meta = root_meta(&frame)?;
                // `price` only where `markPrice` is absent (the Java library's alias).
                let px = exact(data(&frame)?, "markPrice", Some("price"))?;
                sink.push(meta, MdEvent::Mark { inst, px });
            }
            Topic::SpotPrice => {
                let meta = root_meta(&frame)?;
                let px = exact(data(&frame)?, "spotPrice", None)?;
                sink.push(meta, MdEvent::Index { inst, px });
            }
            Topic::FundingRateEstimation => {
                let meta = root_meta(&frame)?;
                let rate_e12 = funding_rate(data(&frame)?)?;
                // The rate per settlement interval; no next time until its unit is evidenced.
                let (interval, next) = (Some(FUNDING_SETTLEMENT_INTERVAL), None);
                let funding = MdEvent::Funding {
                    inst,
                    rate_e12,
                    interval,
                    next,
                };
                sink.push(meta, funding);
            }
        }
        Ok(())
    }

    /// Refused: the codec asks for no HTTP request.
    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed(
            "no Hibachi market-data request is made",
        ))
    }

    /// A book's timer: one waiting for its first Snapshot, or gapped, is resubscribed (a gap
    /// reported for the first); one a resubscribe brought no Snapshot to asks for one reconnect.
    /// Any other tag is stale (the book took a Snapshot, or was unsubscribed) and does nothing.
    fn on_timer(
        &mut self,
        tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        let timer = |b: &&mut OrderBook| b.phase.timer() == Some(tag);
        if let Some(book) = self.books.values_mut().find(timer) {
            self.shared.on_book_timer(book, sink, fx);
        }
    }

    /// None: Hibachi documents no keepalive for the market feed, and the runtime answers the
    /// WebSocket's pings; the consumer's silence window (0033) watches the connection.
    fn keepalive(&self) -> Option<Keepalive> {
        None
    }

    /// Public market data holds no credential.
    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}
