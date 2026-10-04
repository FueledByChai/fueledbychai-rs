//! Paradex public market data over its WebSocket: SBE binary frames ([`sbe`]) for channel data,
//! JSON-RPC text frames for the control plane (docs.paradex.trade, "Binary Encoding (SBE)":
//! subscribe requests, their acknowledgements and errors stay JSON).
//!
//! [`ParadexMd`] subscribes with one JSON-RPC `subscribe` text frame per channel
//! (`bbo.{market}`, `trades.{market}`, `order_book.{market}.{feed_type}@15@50ms`), and decodes
//! `BboEvent` (template 2) into [`MdEvent::Touch`], `TradeEvent` (template 1) into
//! [`MdEvent::Trade`], `BookEvent` (template 3) into book events ([`book`]) and
//! `MarketSummaryEvent` (template 4, `markets_summary.{market}`) into [`MdEvent::Mark`] and
//! [`MdEvent::Funding`] ([`summary`]), each pushed while its feed is subscribed. Heartbeats
//! (template 40) and templates it does not decode are skipped. A subscribe acknowledgement is
//! consumed; a subscribe error is returned as the frame's error and never retried.
//!
//! [`rest`] decodes the REST order book snapshot (`/v1/orderbook/{market}` at depth 15).

pub mod book;
pub mod rest;
pub mod sbe;
pub mod summary;

use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{
    Aggressor, BookId, DecodeError, DecodeScope, Effect, Effects, ExchNs, ExchTsKind, Feed,
    HttpFailure, HttpResponse, HttpTag, InstrumentId, InstrumentSpec, Keepalive, Lots, Lvl,
    MdCodec, MdEvent, MdSink, MonoNs, OpKind, PxExact, RateCharge, RawFrame, SpecTable, StreamId,
    Subscription, Ticks, TimerTag, TouchSourceId, TrafficClass, VenueError, VenueMeta, WallNs,
    WireSlice,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Value, json};

use book::{BookFeed, decode_book};
use sbe::{Block, Message, NULL_I64};
use summary::{TEMPLATE_SUMMARY, decode_summary};

pub use book::{DELTAS, INTERACTIVE_DELTAS};

/// `TradeEvent`'s template id.
pub const TEMPLATE_TRADE: u16 = 1;
/// `BboEvent`'s template id.
pub const TEMPLATE_BBO: u16 = 2;
/// `BookEvent`'s template id.
pub const TEMPLATE_BOOK: u16 = 3;
/// `HeartbeatEvent`'s template id: skipped.
pub const TEMPLATE_HEARTBEAT: u16 = 40;

/// The one touch source: the `bbo.{market}` channel, index 0 of the caps' `touch_sources`.
pub const BBO: TouchSourceId = TouchSourceId(0);

/// The schema's fixed decimal exponent for prices and quantities (`Price8`, `Qty8`).
const EXP: i8 = -8;

/// The channel a subscription is to, as Paradex names it; refused for a feed this adapter does
/// not decode, or an instrument missing from `specs`.
pub fn channel(sub: Subscription, specs: &SpecTable) -> Result<String, VenueError> {
    let spell = |symbol: &str| match sub.feed {
        Feed::Touch(BBO) => Some(format!("bbo.{symbol}")),
        Feed::Trades => Some(format!("trades.{symbol}")),
        // One channel carries both: MarketSummaryEvent has the mark price and the funding rate.
        Feed::Mark | Feed::Funding => Some(format!("markets_summary.{symbol}")),
        Feed::Book(book) => book::book_channel(book, symbol),
        _ => None,
    };
    let unsupported = VenueError::UnsupportedFeed(sub);
    // A feed this adapter does not decode is refused before the instrument is looked up.
    spell("").ok_or(unsupported)?;
    let spec = specs
        .get(sub.inst)
        .ok_or(VenueError::UnknownInstrument(sub.inst))?;
    spell(spec.venue_symbol.as_wire()).ok_or(unsupported)
}

/// What a JSON-RPC request this codec sent asked for.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Request {
    Subscribe,
    Unsubscribe,
}

/// The market-data codec for one connection epoch of one Paradex WebSocket.
#[derive(Debug)]
pub struct ParadexMd {
    stream: StreamId,
    next_id: u64,
    /// Requests sent and not yet answered, by JSON-RPC id.
    pending: BTreeMap<u64, Request>,
    /// The one book channel each market may have on this connection: its first book
    /// subscription here fixes it, and an unsubscribe keeps it, since frames of the old channel
    /// can still arrive and name only their market.
    channel_of: BTreeMap<InstrumentId, BookId>,
    /// The books subscribed now, and their sequences.
    books: BTreeMap<InstrumentId, BookFeed>,
    /// The mark and funding feeds subscribed now: a market's `markets_summary` channel is
    /// subscribed while it has either.
    summary: BTreeSet<Subscription>,
}

impl ParadexMd {
    /// A codec for `stream`, with no request sent.
    pub fn new(stream: StreamId) -> ParadexMd {
        ParadexMd {
            stream,
            next_id: 1,
            pending: BTreeMap::new(),
            channel_of: BTreeMap::new(),
            books: BTreeMap::new(),
            summary: BTreeSet::new(),
        }
    }

    /// Each market's book channel, and the markets with a book subscribed, once `add` (sent
    /// first) and `remove` are taken. A market has at most one book channel on a connection,
    /// since a frame names its market and not its channel: another is refused as a feed this
    /// connection cannot carry, even after the first is removed (Codex r4176866128).
    fn books_after(
        &self,
        add: &[Subscription],
        remove: &[Subscription],
    ) -> Result<(BTreeMap<InstrumentId, BookId>, BTreeSet<InstrumentId>), VenueError> {
        let mut channel_of = self.channel_of.clone();
        let mut active: BTreeSet<_> = self.books.keys().copied().collect();
        for sub in add {
            if let Feed::Book(book) = sub.feed {
                if *channel_of.entry(sub.inst).or_insert(book) != book {
                    return Err(VenueError::UnsupportedFeed(*sub));
                }
                active.insert(sub.inst);
            }
        }
        for sub in remove {
            if let Feed::Book(book) = sub.feed
                && channel_of.get(&sub.inst) == Some(&book)
            {
                active.remove(&sub.inst);
            }
        }
        Ok((channel_of, active))
    }

    /// Consumes a JSON-RPC text frame: an acknowledgement is consumed; an error is returned,
    /// naming the request it answers, and asks for nothing, so nothing is retried.
    fn on_text(&mut self, text: &str) -> Result<(), DecodeError> {
        let reply: Value = serde_json::from_str(text)
            .map_err(|_| DecodeError::Malformed("text frame is not JSON"))?;
        let request = reply
            .get("id")
            .and_then(Value::as_u64)
            .and_then(|id| self.pending.remove(&id));
        if reply.get("result").is_some() {
            return Ok(());
        }
        if reply.get("error").is_none() {
            return Err(DecodeError::Malformed(
                "text frame is neither a reply nor an error",
            ));
        }
        Err(DecodeError::Malformed(match request {
            Some(Request::Subscribe) => "the venue refused a subscribe",
            Some(Request::Unsubscribe) => "the venue refused an unsubscribe",
            None => "the venue reported an error",
        }))
    }
}

impl MdCodec for ParadexMd {
    fn on_open(&mut self, _fx: &mut Effects) {}

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        // Every channel is spelled, and the books checked, before the first push, so a refusal
        // pushes nothing and changes nothing.
        let mut frames = Vec::new();
        let mut summary = self.summary.clone();
        for (request, subs) in [(Request::Subscribe, add), (Request::Unsubscribe, remove)] {
            for sub in subs {
                let channel = channel(*sub, specs)?;
                if summary_feed(sub.feed) && !summary_changes(&mut summary, request, *sub) {
                    continue;
                }
                frames.push((request, channel, sub.inst));
            }
        }
        let (channel_of, active) = self.books_after(add, remove)?;
        self.summary = summary;
        self.books.retain(|inst, _| active.contains(inst));
        for inst in active {
            let book = channel_of[&inst];
            self.books
                .entry(inst)
                .or_insert_with(|| BookFeed::new(book));
        }
        self.channel_of = channel_of;
        for (request, channel, inst) in frames {
            let (id, method) = (self.next_id, request_method(request));
            self.next_id += 1;
            self.pending.insert(id, request);
            let text = json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": { "channel": channel },
                "id": id,
            });
            fx.push(Effect::Send {
                stream: self.stream,
                frame: WireSlice::plain(text.to_string().into_bytes()),
                rpc: None,
                class: TrafficClass::Normal,
                charge: RateCharge::one(OpKind::Subscribe, Some(inst)),
            });
        }
        Ok(())
    }

    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let frame = match f {
            RawFrame::Text(text) => return self.on_text(text),
            RawFrame::Binary(frame) => frame,
        };
        let msg = Message::parse(frame)?;
        let (meta, event) = match msg.header().template_id {
            TEMPLATE_BBO => decode_bbo(&msg, specs)?,
            TEMPLATE_TRADE => decode_trade(&msg, specs)?,
            TEMPLATE_SUMMARY => {
                let s = decode_summary(&msg, specs)?;
                let wanted = |feed| self.summary.contains(&Subscription { inst: s.inst, feed });
                if wanted(Feed::Mark) {
                    let mark = MdEvent::Mark {
                        inst: s.inst,
                        px: s.mark,
                    };
                    sink.push(s.meta, mark);
                }
                if wanted(Feed::Funding) {
                    let funding = MdEvent::Funding {
                        inst: s.inst,
                        rate_e12: s.rate_e12,
                        // MarketSummaryEvent states neither.
                        interval: None,
                        next: None,
                    };
                    sink.push(s.meta, funding);
                }
                return Ok(());
            }
            TEMPLATE_BOOK => {
                let frame = decode_book(&msg, specs)?;
                // A market with no book on this connection (unsubscribed, the frame in flight)
                // has nothing to apply it to.
                if let Some(feed) = self.books.get_mut(&frame.inst()) {
                    feed.apply(frame, self.stream, sink, fx);
                }
                return Ok(());
            }
            // Heartbeats, and every template this adapter does not decode, are skipped: the
            // schema's versioning policy says unknown template ids must be.
            _ => return Ok(()),
        };
        sink.push(meta, event);
        Ok(())
    }

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
            "Paradex market data asks for no HTTP",
        ))
    }

    fn on_timer(
        &mut self,
        _tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) {
    }

    /// None: the server pings every 55 seconds and the WebSocket layer answers with a pong
    /// (docs.paradex.trade, WebSocket "Introduction", ping/pong).
    fn keepalive(&self) -> Option<Keepalive> {
        None
    }
}

/// A feed carried by the `markets_summary` channel.
fn summary_feed(feed: Feed) -> bool {
    matches!(feed, Feed::Mark | Feed::Funding)
}

/// Takes `request` for summary feed `sub` into `summary`; true when the market's
/// `markets_summary` channel needs the request sent: the first of its feeds subscribed, or the
/// last subscribed one removed.
fn summary_changes(
    summary: &mut BTreeSet<Subscription>,
    request: Request,
    sub: Subscription,
) -> bool {
    let any = |summary: &BTreeSet<Subscription>| {
        [Feed::Mark, Feed::Funding].into_iter().any(|feed| {
            summary.contains(&Subscription {
                inst: sub.inst,
                feed,
            })
        })
    };
    match request {
        Request::Subscribe => {
            let had = any(summary);
            summary.insert(sub);
            !had
        }
        Request::Unsubscribe => summary.remove(&sub) && !any(summary),
    }
}

fn request_method(request: Request) -> &'static str {
    match request {
        Request::Subscribe => "subscribe",
        Request::Unsubscribe => "unsubscribe",
    }
}

/// `BboEvent`: ts@0, seq@8, bidPrice@16, bidSize@24, askPrice@32, askSize@40, then `market`.
fn decode_bbo(msg: &Message<'_>, specs: &SpecTable) -> Result<(VenueMeta, MdEvent), DecodeError> {
    let block = msg.block();
    let spec = frame_market(msg, specs)?;
    let bid = level(spec, &block, 16, "bbo bid")?;
    let ask = level(spec, &block, 32, "bbo ask")?;
    let meta = VenueMeta {
        exch_ts: Some(micros(required(&block, 0, "bbo ts")?)?),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq(required(&block, 8, "bbo seq")?)?),
    };
    let event = MdEvent::Touch {
        inst: spec.id,
        bid,
        ask,
        source: BBO,
    };
    Ok((meta, event))
}

/// `TradeEvent`: seq@8, side@24, price@25, size@33, createdAt@41, then `market`. The int64
/// `tradeId`@16 is not read: the schema deprecates it as the truncated low 64 bits of
/// Paradex's 28-digit trade id.
fn decode_trade(msg: &Message<'_>, specs: &SpecTable) -> Result<(VenueMeta, MdEvent), DecodeError> {
    let block = msg.block();
    let spec = frame_market(msg, specs)?;
    let aggressor = match block.u8_at(24) {
        Some(1) => Aggressor::Buyer,
        Some(2) => Aggressor::Seller,
        Some(254) => Aggressor::Unknown,
        _ => return Err(DecodeError::Malformed("trade side")),
    };
    let px = price(spec, required(&block, 25, "trade price")?)?;
    let qty = lots(spec, required(&block, 33, "trade size")?)?;
    // 0 is the schema's "no orderbook sequence" (block-trade legs, position transfers).
    let seq = Some(seq(required(&block, 8, "trade seq")?)?).filter(|&s| s != 0);
    let meta = VenueMeta {
        exch_ts: Some(micros(required(&block, 41, "trade createdAt")?)?),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: seq,
    };
    let event = MdEvent::Trade {
        inst: spec.id,
        id: None,
        aggressor,
        px,
        qty,
    };
    Ok((meta, event))
}

/// The spec of a bbo or trade frame's `market`, the first variable-length field of both.
fn frame_market<'s>(
    msg: &Message<'_>,
    specs: &'s SpecTable,
) -> Result<&'s InstrumentSpec, DecodeError> {
    let symbol = msg.tail().var_str()?;
    market(symbol.ok_or(DecodeError::Malformed("SBE market"))?, specs)
}

/// The spec of the market spelled `symbol`.
fn market<'s>(symbol: &str, specs: &'s SpecTable) -> Result<&'s InstrumentSpec, DecodeError> {
    specs
        .by_symbol(symbol)
        .ok_or(DecodeError::UnknownInstrument)
}

/// A root field the decoder needs, refused when the frame's block ends before it.
fn required(block: &Block<'_>, offset: usize, what: &'static str) -> Result<i64, DecodeError> {
    block.i64_at(offset).ok_or(DecodeError::Malformed(what))
}

/// One side of a `BboEvent` from the price at `offset` and the size after it: `None` when both
/// are null, refused when one is.
fn level(
    spec: &InstrumentSpec,
    block: &Block<'_>,
    offset: usize,
    what: &'static str,
) -> Result<Option<Lvl>, DecodeError> {
    let px = required(block, offset, what)?;
    let qty = required(block, offset + 8, what)?;
    match (px, qty) {
        (NULL_I64, NULL_I64) => Ok(None),
        (NULL_I64, _) | (_, NULL_I64) => Err(DecodeError::Malformed(what)),
        (px, qty) => Ok(Some(Lvl {
            px: price(spec, px)?,
            qty: lots(spec, qty)?,
        })),
    }
}

/// A `Price8` mantissa as ticks on the instrument's grid; refused off the grid or when null.
fn price(spec: &InstrumentSpec, mantissa: i64) -> Result<Ticks, DecodeError> {
    let off = DecodeError::Malformed("price off the instrument's grid");
    if mantissa == NULL_I64 {
        return Err(off);
    }
    spec.price_grid
        .ticks_exact(PxExact::new(mantissa, EXP))
        .ok_or(off)
}

/// A `Qty8` mantissa as lots of the instrument's size step; refused off the step, negative or
/// null.
fn lots(spec: &InstrumentSpec, mantissa: i64) -> Result<Lots, DecodeError> {
    lots_of(spec, Decimal::new(mantissa, EXP.unsigned_abs().into()))
}

/// A size as lots of the instrument's size step; refused off the step or negative.
fn lots_of(spec: &InstrumentSpec, value: Decimal) -> Result<Lots, DecodeError> {
    let off = DecodeError::Malformed("size off the instrument's size step");
    let step = spec.size_step.get();
    let count = value.checked_div(step).ok_or(off)?;
    if !count.fract().is_zero() || count.checked_mul(step) != Some(value) {
        return Err(off);
    }
    count.to_i64().and_then(Lots::new).ok_or(off)
}

/// A timestamp in microseconds as nanoseconds; refused where it does not fit.
fn micros(us: i64) -> Result<ExchNs, DecodeError> {
    us.checked_mul(1_000)
        .map(ExchNs)
        .ok_or(DecodeError::Malformed("timestamp out of range"))
}

/// A sequence number; refused when negative.
fn seq(seq: i64) -> Result<u64, DecodeError> {
    u64::try_from(seq).map_err(|_| DecodeError::Malformed("negative seq"))
}
