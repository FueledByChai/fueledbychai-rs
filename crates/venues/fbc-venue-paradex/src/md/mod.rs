//! Paradex public market data over its WebSocket: SBE binary frames ([`sbe`]) for channel data,
//! JSON-RPC text frames for the control plane (docs.paradex.trade, "Binary Encoding (SBE)":
//! subscribe requests, their acknowledgements and errors stay JSON).
//!
//! [`ParadexMd`] subscribes with one JSON-RPC `subscribe` text frame per channel
//! (`bbo.{market}`, `bbo.{market}.interactive`, `trades.{market}`,
//! `order_book.{market}.{feed_type}`), and decodes `BboEvent` (template 2) into
//! [`MdEvent::Touch`] of the touch source the market holds on the connection ([`BBO`] or
//! [`BBO_INTERACTIVE`]: both channels carry the same template and a frame names only its
//! market, so a connection carries at most one of a market's, decision 0076), `TradeEvent`
//! (template 1) into [`MdEvent::Trade`], `BookEvent` (template 3) into book events ([`book`]) and
//! `MarketSummaryEvent` (template 4, `markets_summary.{market}`) into [`MdEvent::Mark`] and
//! [`MdEvent::Funding`] ([`summary`]), each pushed while its feed is subscribed. Heartbeats
//! (template 40) and templates it does not decode are skipped. A subscribe acknowledgement is
//! consumed; a subscribe error is reported as [`FeedHealth::Refused`] for each subscription the
//! channel was to carry, and never retried (decision 0042).
//!
//! [`rest`] decodes the REST order book snapshot (`/v1/orderbook/{market}` at depth 15).

pub mod book;
pub mod rest;
pub mod sbe;
pub mod summary;

use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{
    Aggressor, BookId, DecodeError, DecodeScope, Effect, Effects, ExchNs, ExchTsKind, Feed,
    FeedHealth, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId,
    InstrumentSpec, Keepalive, Lots, Lvl, MdCodec, MdEvent, MdSink, MonoNs, OpKind, PxExact,
    RateCharge, RawFrame, SpecTable, StreamId, Subscription, Ticks, TimerTag, TouchSourceId,
    TrafficClass, VenueError, VenueMeta, WallNs, WireSlice,
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

/// The `bbo.{market}` channel, the public book's touch: index 0 of the caps' `touch_sources`.
pub const BBO: TouchSourceId = TouchSourceId(0);
/// The `bbo.{market}.interactive` channel, the touch including RPI orders: index 1 of the caps'
/// `touch_sources` (decision 0076).
pub const BBO_INTERACTIVE: TouchSourceId = TouchSourceId(1);

/// The channel name of touch `source` of the market spelled `symbol`, or `None` for an
/// undeclared one. The interactive touch is exactly `bbo.{market}.interactive`: the venue
/// acknowledges `bbo.interactive.{market}` and `bbo.{market}@interactive` too but streams
/// nothing on them (decision 0076).
fn touch_channel(source: TouchSourceId, symbol: &str) -> Option<String> {
    match source {
        BBO => Some(format!("bbo.{symbol}")),
        BBO_INTERACTIVE => Some(format!("bbo.{symbol}.interactive")),
        _ => None,
    }
}

/// The schema's fixed decimal exponent for prices and quantities (`Price8`, `Qty8`).
const EXP: i8 = -8;

/// The channel a subscription is to, as Paradex names it; refused for a feed this adapter does
/// not decode, or an instrument missing from `specs`.
pub fn channel(sub: Subscription, specs: &SpecTable) -> Result<String, VenueError> {
    let spell = |symbol: &str| match sub.feed {
        Feed::Touch(source) => touch_channel(source, symbol),
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

/// A JSON-RPC request this codec sent and the venue has not answered yet.
#[derive(Clone, Eq, PartialEq, Debug)]
struct Sent {
    request: Request,
    /// The subscription the request was sent for.
    sub: Subscription,
    channel: String,
}

/// The market-data codec for one connection epoch of one Paradex WebSocket.
#[derive(Debug)]
pub struct ParadexMd {
    stream: StreamId,
    next_id: u64,
    /// Requests sent and not yet answered, by JSON-RPC id.
    pending: BTreeMap<u64, Sent>,
    /// The one book channel each market may have on this connection: its first book
    /// subscription here fixes it, and an unsubscribe keeps it, since frames of the old channel
    /// can still arrive and name only their market.
    channel_of: BTreeMap<InstrumentId, BookId>,
    /// The one touch source each market may have on this connection, fixed and kept as
    /// `channel_of` is: a bbo frame is the touch of the source its market holds here
    /// (decision 0076).
    touch_of: BTreeMap<InstrumentId, TouchSourceId>,
    /// The books subscribed now, and their sequences.
    books: BTreeMap<InstrumentId, BookFeed>,
    /// The mark and funding feeds subscribed now: a market's `markets_summary` channel is
    /// subscribed while it has either.
    summary: BTreeSet<Subscription>,
    /// Touch and trade feeds the venue refused and nothing has subscribed since: their frames
    /// still in flight are not pushed.
    refused: BTreeSet<Subscription>,
    /// The id of the latest subscribe sent for each channel, answered or not: a refusal of an
    /// older one leaves the state a newer one set up.
    latest: BTreeMap<String, u64>,
}

impl ParadexMd {
    /// A codec for `stream`, with no request sent.
    pub fn new(stream: StreamId) -> ParadexMd {
        ParadexMd {
            stream,
            next_id: 1,
            pending: BTreeMap::new(),
            channel_of: BTreeMap::new(),
            touch_of: BTreeMap::new(),
            books: BTreeMap::new(),
            summary: BTreeSet::new(),
            refused: BTreeSet::new(),
            latest: BTreeMap::new(),
        }
    }

    /// Each market's book channel and touch source, and the markets with a book subscribed,
    /// once `add` (sent first) and `remove` are taken. A market has at most one book channel
    /// and one touch source on a connection, since a frame names its market and not its
    /// channel: another is refused as a feed this connection cannot carry, even after the
    /// first is removed (Codex r4176866128, decision 0076).
    fn channels_after(
        &self,
        add: &[Subscription],
        remove: &[Subscription],
    ) -> Result<Channels, VenueError> {
        let mut channel_of = self.channel_of.clone();
        let mut touch_of = self.touch_of.clone();
        let mut active: BTreeSet<_> = self.books.keys().copied().collect();
        for sub in add {
            // Whether the market already holds another channel of the kind here.
            let other = match sub.feed {
                Feed::Book(book) => {
                    active.insert(sub.inst);
                    *channel_of.entry(sub.inst).or_insert(book) != book
                }
                Feed::Touch(source) => *touch_of.entry(sub.inst).or_insert(source) != source,
                _ => false,
            };
            if other {
                return Err(VenueError::UnsupportedFeed(*sub));
            }
        }
        for sub in remove {
            if let Feed::Book(book) = sub.feed
                && channel_of.get(&sub.inst) == Some(&book)
            {
                active.remove(&sub.inst);
            }
        }
        Ok(Channels {
            channel_of,
            touch_of,
            active,
        })
    }

    /// Consumes a JSON-RPC text frame: an acknowledgement is consumed; a refused subscribe is
    /// reported ([`Self::refuse`]); any other error is returned, naming the request it
    /// answers. None asks for anything, so nothing is retried.
    fn on_text(&mut self, text: &str, sink: &mut dyn MdSink) -> Result<(), DecodeError> {
        let reply: Value = serde_json::from_str(text)
            .map_err(|_| DecodeError::Malformed("text frame is not JSON"))?;
        let id = reply.get("id").and_then(Value::as_u64);
        let sent = id.and_then(|id| Some((id, self.pending.remove(&id)?)));
        if reply.get("result").is_some() {
            return Ok(());
        }
        if reply.get("error").is_none() {
            return Err(DecodeError::Malformed(
                "text frame is neither a reply nor an error",
            ));
        }
        match sent {
            Some((id, sent)) if sent.request == Request::Subscribe => {
                self.refuse(id, sent, sink);
                Ok(())
            }
            Some(_) => Err(DecodeError::Malformed("the venue refused an unsubscribe")),
            None => Err(DecodeError::Malformed("the venue reported an error")),
        }
    }

    /// The venue refused subscribe request `id`, `sent`: pushes [`FeedHealth::Refused`] for
    /// each subscription its channel was to carry (a market's `markets_summary` channel carries
    /// its mark and its funding), which the codec then holds no longer, so their frames are not
    /// pushed or applied and only a new subscription sends the channel again. When a later
    /// subscribe to the same channel (a higher id) was sent, answered or not, this refusal
    /// answers the older request only: it names the subscription it was sent for and keeps the
    /// state the later one set up (Codex r4182919464, r4183051811).
    fn refuse(&mut self, id: u64, sent: Sent, sink: &mut dyn MdSink) {
        let again = self
            .latest
            .get(&sent.channel)
            .is_some_and(|&last| last > id);
        let mut refused = BTreeSet::from([sent.sub]);
        if !again {
            let inst = sent.sub.inst;
            match sent.sub.feed {
                Feed::Mark | Feed::Funding => {
                    let carried = |s: &Subscription| s.inst == inst;
                    refused.extend(self.summary.iter().copied().filter(carried));
                    self.summary.retain(|s| !carried(s));
                }
                Feed::Book(_) => {
                    self.books.remove(&inst);
                }
                // Touch and trades (Codex r4182919469).
                _ => {
                    self.refused.insert(sent.sub);
                }
            }
        }
        for Subscription { inst, feed } in refused {
            let h = FeedHealth::Refused;
            sink.push(VenueMeta::NONE, MdEvent::Health { inst, feed, h });
        }
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
                frames.push((request, channel, *sub));
            }
        }
        let Channels {
            channel_of,
            touch_of,
            active,
        } = self.channels_after(add, remove)?;
        self.summary = summary;
        self.touch_of = touch_of;
        self.books.retain(|inst, _| active.contains(inst));
        for inst in active {
            let book = channel_of[&inst];
            self.books
                .entry(inst)
                .or_insert_with(|| BookFeed::new(book));
        }
        self.channel_of = channel_of;
        for sub in add {
            self.refused.remove(sub);
        }
        for (request, channel, sub) in frames {
            let (id, method) = (self.next_id, request_method(request));
            self.next_id += 1;
            if request == Request::Subscribe {
                self.latest.insert(channel.clone(), id);
            }
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
                charge: RateCharge::one(OpKind::Subscribe, Some(sub.inst)),
            });
            self.pending.insert(
                id,
                Sent {
                    request,
                    sub,
                    channel,
                },
            );
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
            RawFrame::Text(text) => return self.on_text(text, sink),
            RawFrame::Binary(frame) => frame,
        };
        let msg = Message::parse(frame)?;
        let (meta, event, sub) = match msg.header().template_id {
            TEMPLATE_BBO => decode_bbo(&msg, specs, &self.touch_of)?,
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
        if !self.refused.contains(&sub) {
            sink.push(meta, event);
        }
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

    /// None: public market data, from public channels and the unauthenticated `/orderbook`.
    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

/// What a subscribe leaves each market's channels on a connection.
struct Channels {
    /// Each market's one book channel.
    channel_of: BTreeMap<InstrumentId, BookId>,
    /// Each market's one touch source.
    touch_of: BTreeMap<InstrumentId, TouchSourceId>,
    /// The markets with a book subscribed.
    active: BTreeSet<InstrumentId>,
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

/// A decoded bbo or trade frame: what the venue said, the event, and the subscription it is
/// data of.
type Decoded = (VenueMeta, MdEvent, Subscription);

/// `BboEvent`: ts@0, seq@8, bidPrice@16, bidSize@24, askPrice@32, askSize@40, then `market`.
/// The touch is of the source `touch_of` holds for the market, [`BBO`] where it holds none.
fn decode_bbo(
    msg: &Message<'_>,
    specs: &SpecTable,
    touch_of: &BTreeMap<InstrumentId, TouchSourceId>,
) -> Result<Decoded, DecodeError> {
    let block = msg.block();
    let spec = frame_market(msg, specs)?;
    let bid = level(spec, &block, 16, "bbo bid")?;
    let ask = level(spec, &block, 32, "bbo ask")?;
    let meta = VenueMeta {
        exch_ts: Some(micros(required(&block, 0, "bbo ts")?)?),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq(required(&block, 8, "bbo seq")?)?),
    };
    let source = touch_of.get(&spec.id).copied().unwrap_or(BBO);
    let event = MdEvent::Touch {
        inst: spec.id,
        bid,
        ask,
        source,
    };
    let feed = Feed::Touch(source);
    Ok((
        meta,
        event,
        Subscription {
            inst: spec.id,
            feed,
        },
    ))
}

/// `TradeEvent`: seq@8, side@24, price@25, size@33, createdAt@41, then `market`. The int64
/// `tradeId`@16 is not read: the schema deprecates it as the truncated low 64 bits of
/// Paradex's 28-digit trade id.
fn decode_trade(msg: &Message<'_>, specs: &SpecTable) -> Result<Decoded, DecodeError> {
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
    let feed = Feed::Trades;
    Ok((
        meta,
        event,
        Subscription {
            inst: spec.id,
            feed,
        },
    ))
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
pub(crate) fn market<'s>(
    symbol: &str,
    specs: &'s SpecTable,
) -> Result<&'s InstrumentSpec, DecodeError> {
    specs
        .by_symbol(symbol)
        .ok_or(DecodeError::UnknownInstrument)
}

/// A root field the decoder needs, refused when the frame's block ends before it.
pub(crate) fn required(
    block: &Block<'_>,
    offset: usize,
    what: &'static str,
) -> Result<i64, DecodeError> {
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
pub(crate) fn price(spec: &InstrumentSpec, mantissa: i64) -> Result<Ticks, DecodeError> {
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
pub(crate) fn lots(spec: &InstrumentSpec, mantissa: i64) -> Result<Lots, DecodeError> {
    lots_of(spec, Decimal::new(mantissa, EXP.unsigned_abs().into()))
}

/// A size as lots of the instrument's size step; refused off the step or negative.
pub(crate) fn lots_of(spec: &InstrumentSpec, value: Decimal) -> Result<Lots, DecodeError> {
    let off = DecodeError::Malformed("size off the instrument's size step");
    let step = spec.size_step.get();
    let count = value.checked_div(step).ok_or(off)?;
    if !count.fract().is_zero() || count.checked_mul(step) != Some(value) {
        return Err(off);
    }
    count.to_i64().and_then(Lots::new).ok_or(off)
}

/// A timestamp in microseconds as nanoseconds; refused where it does not fit.
pub(crate) fn micros(us: i64) -> Result<ExchNs, DecodeError> {
    us.checked_mul(1_000)
        .map(ExchNs)
        .ok_or(DecodeError::Malformed("timestamp out of range"))
}

/// A sequence number; refused when negative.
pub(crate) fn seq(seq: i64) -> Result<u64, DecodeError> {
    u64::try_from(seq).map_err(|_| DecodeError::Malformed("negative seq"))
}
