//! The market-data codec: live subscriptions on a combined-stream connection, `bookTicker`
//! touches, partial-depth snapshots and the diff-depth book anchored on a REST snapshot
//! (`diff.rs`; decisions 0002, 0014).
//!
//! Every frame is decoded whole before anything is pushed, so a frame that fails pushes nothing
//! (`MdSink`'s contract). Prices must lie on the instrument's grid and quantities on its size
//! step exactly; nothing is rounded.

use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{
    BookSide, ConfigError, DecodeError, DecodeScope, Effect, Effects, ExchNs, ExchTsKind, Feed,
    HttpFailure, HttpResponse, HttpTag, InstrumentId, InstrumentSpec, Keepalive, Lots, Lvl,
    MdCodec, MdEvent, MdSink, MonoNs, OpKind, PxExact, RateCharge, RawFrame, SpecTable, StreamId,
    Subscription, Ticks, TimerTag, TrafficClass, VenueError, VenueMeta, WallNs, WireSlice,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value};

use crate::caps::DIFF_CHANNEL;
use crate::config::Settings;
use crate::diff::{DiffBooks, decode_event};
use crate::{BOOK_DIFF, BOOK_PARTIAL, TOUCH_BOOK_TICKER};

/// One SUBSCRIBE or UNSUBSCRIBE request: a message on the connection, counted against its
/// incoming-message limit. It names no single instrument, and Binance counts no limit per pair.
const SUBSCRIBE_CHARGE: RateCharge = RateCharge::one(OpKind::Subscribe, None);

/// The stream name of `sub` (`<symbol>@<channel>`, the symbol lowercased), or why it cannot be
/// subscribed: a feed this adapter does not decode, or an instrument missing from `specs`.
pub(crate) fn stream_name(
    settings: &Settings,
    specs: &SpecTable,
    sub: Subscription,
) -> Result<String, VenueError> {
    let channel = match sub.feed {
        Feed::Touch(TOUCH_BOOK_TICKER) => "bookTicker",
        Feed::Book(BOOK_PARTIAL) => settings.depth.name,
        Feed::Book(BOOK_DIFF) => DIFF_CHANNEL,
        _ => return Err(VenueError::UnsupportedFeed(sub)),
    };
    let spec = specs
        .get(sub.inst)
        .ok_or(VenueError::UnknownInstrument(sub.inst))?;
    let symbol = spec.venue_symbol.as_wire().to_ascii_lowercase();
    Ok(format!("{symbol}@{channel}"))
}

/// What a request id was sent for.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Method {
    Subscribe,
    Unsubscribe,
}

impl Method {
    fn wire(self) -> &'static str {
        match self {
            Method::Subscribe => "SUBSCRIBE",
            Method::Unsubscribe => "UNSUBSCRIBE",
        }
    }
}

/// The codec for one connection epoch of one combined-stream endpoint.
pub(crate) struct BinanceUsdmMd {
    stream: StreamId,
    settings: Result<Settings, ConfigError>,
    /// The next request id: Binance takes an unsigned integer and echoes it in the reply.
    next_id: u64,
    /// Requests sent and not yet answered.
    pending: BTreeMap<u64, Method>,
    /// The epoch of each instrument's last partial-depth snapshot.
    epochs: BTreeMap<InstrumentId, u32>,
    /// The diff-depth books subscribed.
    diff: DiffBooks,
}

/// What a stream carries.
#[derive(Copy, Clone)]
enum Kind {
    Ticker,
    /// Partial depth, at most this many levels per side.
    Partial(u16),
    Diff,
}

impl BinanceUsdmMd {
    pub fn new(stream: StreamId, settings: Result<Settings, ConfigError>) -> BinanceUsdmMd {
        BinanceUsdmMd {
            stream,
            settings,
            next_id: 1,
            pending: BTreeMap::new(),
            epochs: BTreeMap::new(),
            diff: DiffBooks::default(),
        }
    }

    /// The stream names of `subs`, each once, in order of first appearance.
    fn names(
        settings: &Settings,
        specs: &SpecTable,
        subs: &[Subscription],
    ) -> Result<Vec<String>, VenueError> {
        let mut seen = BTreeSet::new();
        let mut names = Vec::new();
        for sub in subs {
            let name = stream_name(settings, specs, *sub)?;
            if seen.insert(name.clone()) {
                names.push(name);
            }
        }
        Ok(names)
    }

    /// Asks for `method` on `names` as one request with the next id.
    fn request(&mut self, method: Method, names: Vec<String>, fx: &mut Effects) {
        let id = self.next_id;
        self.next_id += 1;
        self.pending.insert(id, method);
        let mut body = Map::new();
        body.insert("method".into(), Value::from(method.wire()));
        body.insert("params".into(), Value::from(names));
        body.insert("id".into(), Value::from(id));
        let frame = Value::Object(body).to_string().into_bytes();
        fx.push(Effect::Send {
            stream: self.stream,
            frame: WireSlice::plain(frame),
            rpc: None,
            class: TrafficClass::Normal,
            charge: SUBSCRIBE_CHARGE,
        });
    }

    /// A reply to a request: `{"result":null,"id":n}`, or an error `{"code":..,"msg":..}`.
    fn on_reply(&mut self, reply: &Map<String, Value>) -> Result<(), DecodeError> {
        let id = reply.get("id").and_then(Value::as_u64);
        if reply.contains_key("code") {
            if let Some(id) = id {
                self.pending.remove(&id);
            }
            return Err(DecodeError::Malformed(
                "the venue refused a subscription request",
            ));
        }
        let id = id.ok_or(DecodeError::Malformed("a reply without its id"))?;
        if !self.pending.contains_key(&id) {
            return Err(DecodeError::Malformed("a reply to no pending request"));
        }
        // SUBSCRIBE and UNSUBSCRIBE answer `"result": null`.
        if reply.get("result") != Some(&Value::Null) {
            return Err(DecodeError::Malformed("a reply without its result"));
        }
        self.pending.remove(&id);
        Ok(())
    }

    /// One market-data message on `stream`.
    fn on_data(
        &mut self,
        stream: &str,
        data: &Map<String, Value>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let (symbol, channel) = stream
            .split_once('@')
            .ok_or(DecodeError::Malformed("stream"))?;
        let depth = self.settings.as_ref().ok().map(|s| s.depth);
        let (event, kind) = match channel {
            "bookTicker" => ("bookTicker", Kind::Ticker),
            DIFF_CHANNEL => ("depthUpdate", Kind::Diff),
            _ => match depth {
                Some(depth) if channel == depth.name => {
                    ("depthUpdate", Kind::Partial(depth.levels))
                }
                _ => {
                    return Err(DecodeError::Malformed(
                        "a stream this codec did not subscribe",
                    ));
                }
            },
        };
        if data.get("e").and_then(Value::as_str) != Some(event) {
            return Err(DecodeError::Malformed("e"));
        }
        let wire = data
            .get("s")
            .and_then(Value::as_str)
            .ok_or(DecodeError::Malformed("s"))?;
        let spec = specs
            .by_symbol(wire)
            .ok_or(DecodeError::UnknownInstrument)?;
        if !wire.eq_ignore_ascii_case(symbol) {
            return Err(DecodeError::Malformed("the stream names another symbol"));
        }
        let meta = meta(data)?;
        match kind {
            Kind::Diff => {
                let ev = decode_event(spec, data, meta)?;
                self.diff.on_event(spec.id, ev, sink, fx);
            }
            Kind::Ticker => {
                let touch = MdEvent::Touch {
                    inst: spec.id,
                    bid: touch_side(spec, data, "b", "B")?,
                    ask: touch_side(spec, data, "a", "A")?,
                    source: TOUCH_BOOK_TICKER,
                };
                sink.push(meta, touch);
            }
            Kind::Partial(max) => {
                let bids = side(spec, data, "b", max)?;
                let asks = side(spec, data, "a", max)?;
                let epoch = self.epochs.entry(spec.id).or_insert(0);
                *epoch = epoch.wrapping_add(1);
                let (inst, book) = (spec.id, BOOK_PARTIAL);
                let epoch = *epoch;
                sink.push(meta, MdEvent::BookSnapshotBegin { inst, book, epoch });
                for (side, levels) in [(BookSide::Bid, &bids), (BookSide::Ask, &asks)] {
                    for lvl in levels {
                        let (px, qty) = (lvl.px, lvl.qty);
                        sink.push(
                            meta,
                            MdEvent::Level {
                                inst,
                                book,
                                side,
                                px,
                                qty,
                            },
                        );
                    }
                }
                // The levels between the deepest bid and the deepest ask are all known; what
                // lies beyond them is not.
                let prices = bids.iter().chain(&asks).map(|l| l.px);
                if let (Some(lo), Some(hi)) = (prices.clone().min(), prices.max()) {
                    sink.push(meta, MdEvent::Window { inst, book, lo, hi });
                }
                sink.push(meta, MdEvent::BookSnapshotEnd { inst, book });
            }
        }
        Ok(())
    }
}

/// The update id `u` as the venue sequence, and the transaction time `T` (milliseconds) as the
/// exchange time.
fn meta(data: &Map<String, Value>) -> Result<VenueMeta, DecodeError> {
    let seq = data.get("u").and_then(Value::as_u64);
    let seq = seq.ok_or(DecodeError::Malformed("u"))?;
    let ts = data.get("T").and_then(Value::as_i64);
    let ns = ts.and_then(|ms| ms.checked_mul(1_000_000));
    let ns = ns.ok_or(DecodeError::Malformed("T"))?;
    Ok(VenueMeta {
        exch_ts: Some(ExchNs(ns)),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: Some(seq),
    })
}

/// A price string on `spec`'s grid, exactly.
fn price(
    spec: &InstrumentSpec,
    text: Option<&Value>,
    key: &'static str,
) -> Result<Ticks, DecodeError> {
    let bad = DecodeError::Malformed(key);
    let px: PxExact = text
        .and_then(Value::as_str)
        .ok_or(bad)?
        .parse()
        .map_err(|_| bad)?;
    spec.price_grid.ticks_exact(px).ok_or(bad)
}

/// A quantity string in whole lots of `spec`'s size step, exactly.
fn quantity(
    spec: &InstrumentSpec,
    text: Option<&Value>,
    key: &'static str,
) -> Result<Lots, DecodeError> {
    let bad = DecodeError::Malformed(key);
    let qty = Decimal::from_str_exact(text.and_then(Value::as_str).ok_or(bad)?).map_err(|_| bad)?;
    let lots = qty.checked_div(spec.size_step.get()).ok_or(bad)?;
    if !lots.fract().is_zero() {
        return Err(bad);
    }
    lots.to_i64().and_then(Lots::new).ok_or(bad)
}

/// One side of a touch; a side with no quantity is empty.
fn touch_side(
    spec: &InstrumentSpec,
    data: &Map<String, Value>,
    px_key: &'static str,
    qty_key: &'static str,
) -> Result<Option<Lvl>, DecodeError> {
    let px = price(spec, data.get(px_key), px_key)?;
    let qty = quantity(spec, data.get(qty_key), qty_key)?;
    Ok((qty != Lots::ZERO).then_some(Lvl { px, qty }))
}

/// One side of a partial-depth message: `[["price","qty"], ...]`, at most `max` levels.
fn side(
    spec: &InstrumentSpec,
    data: &Map<String, Value>,
    key: &'static str,
    max: u16,
) -> Result<Vec<Lvl>, DecodeError> {
    let bad = DecodeError::Malformed(key);
    let levels = data.get(key).and_then(Value::as_array).ok_or(bad)?;
    if levels.len() > usize::from(max) {
        return Err(DecodeError::Malformed(match key {
            "b" => "b: more levels than the channel carries",
            _ => "a: more levels than the channel carries",
        }));
    }
    level_list(spec, levels, key)
}

/// A list of levels, `[["price","qty"], ...]`, each on `spec`'s grid and size step exactly;
/// a fault names `key`.
pub(crate) fn level_list(
    spec: &InstrumentSpec,
    levels: &[Value],
    key: &'static str,
) -> Result<Vec<Lvl>, DecodeError> {
    levels
        .iter()
        .map(|level| match level.as_array().map(Vec::as_slice) {
            Some([px, qty]) => Ok(Lvl {
                px: price(spec, Some(px), key)?,
                qty: quantity(spec, Some(qty), key)?,
            }),
            _ => Err(DecodeError::Malformed(key)),
        })
        .collect()
}

impl MdCodec for BinanceUsdmMd {
    /// Nothing to send: the subscriptions follow.
    fn on_open(&mut self, _fx: &mut Effects) {}

    /// One SUBSCRIBE for `add` and one UNSUBSCRIBE for `remove`, each naming every stream once,
    /// each under its own request id; none for an empty list. Every subscription is checked
    /// before the first push, so a refusal sends nothing.
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
        let adds = Self::names(settings, specs, add)?;
        let removes = Self::names(settings, specs, remove)?;
        // Every name is checked, so every instrument is in `specs`.
        let diff = |sub: &&Subscription| sub.feed == Feed::Book(BOOK_DIFF);
        for sub in add.iter().filter(diff) {
            if let Some(spec) = specs.get(sub.inst) {
                let symbol = spec.venue_symbol.as_wire();
                self.diff.subscribe(sub.inst, symbol, &settings.snapshot);
            }
        }
        for sub in remove.iter().filter(diff) {
            self.diff.unsubscribe(sub.inst);
        }
        for (method, names) in [(Method::Subscribe, adds), (Method::Unsubscribe, removes)] {
            if !names.is_empty() {
                self.request(method, names, fx);
            }
        }
        Ok(())
    }

    /// A combined-stream message `{"stream":..,"data":..}`, or a reply to a request.
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
        match frame.get("stream") {
            Some(stream) => {
                let stream = stream.as_str().ok_or(DecodeError::Malformed("stream"))?;
                let data = frame.get("data").and_then(Value::as_object);
                let data = data.ok_or(DecodeError::Malformed("data"))?;
                self.on_data(stream, data, specs, sink, fx)
            }
            None => self.on_reply(&frame),
        }
    }

    /// The response to a diff-depth book's snapshot request, or why none came.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.diff.on_http(tag, resp, specs, sink, fx)
    }

    /// A diff-depth book's retry timer: it asks for its snapshot again.
    fn on_timer(
        &mut self,
        tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        _sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        self.diff.on_timer(tag, fx);
    }

    /// None: Binance pings the connection every 3 minutes and the WebSocket layer answers with
    /// a pong; the codec sends nothing of its own.
    fn keepalive(&self) -> Option<Keepalive> {
        None
    }
}
