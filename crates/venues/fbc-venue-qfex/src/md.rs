//! Sans-IO decoder for QFEX's public MDS WebSocket.
//!
//! Besides `bbo`, `level2` and `trade`, it decodes three reference channels, each value exact and
//! never rounded to a grid:
//!
//! - [`mark_price`](https://docs.qfex.com/websocket/channels/mds/mark_price): `price` as
//!   [`MdEvent::Mark`]. The page's example has no `sequence`; live frames carry one.
//! - [`underlier`](https://docs.qfex.com/websocket/channels/mds/underlier): `price` as
//!   [`MdEvent::Index`], the price QFEX funds against, only where `source` is `external`.
//!   [`MdEvent::Index`] cannot say where a price came from, so an `internal` one (which the page
//!   gives outside extended hours, and the 2026-11-04 capture shows within them too) pushes no
//!   event and is counted ([`QfexMdCounts::internal_underliers`]): the index pauses rather than
//!   carry a price the consumer cannot classify.
//! - [`funding`](https://docs.qfex.com/websocket/channels/mds/funding_rate) (the subscription
//!   channel; not `funding_rate`): `funding_rate`, a decimal fraction for the hourly settlement
//!   ([funding](https://docs.qfex.com/qfex/funding);
//!   [market_stats](https://docs.qfex.com/websocket/channels/mds/market_stats): "decimal
//!   strings, not percentage strings"; the capture, `fixtures/qfex/README.md`), as
//!   [`MdEvent::Funding`] in units of 1e-12 exactly: a rate finer than 1e-12 or outside `i64`
//!   refuses the frame. No frame states the interval, so `interval` is `None`. `next` is `None`:
//!   the capture shows `time` + `time_remaining` falls seconds short of the settlement, since
//!   `time_remaining` counts from when the frame was sent. Neither `time_remaining` nor the live
//!   frames' undocumented `annualised_funding_rate` is read.

use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{
    Aggressor, BookSide, DecodeError, DecodeScope, Effect, Effects, ExchNs, ExchTsKind, Feed,
    FeedHealth, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId,
    InstrumentSpec, Keepalive, Lots, Lvl, MdCodec, MdEvent, MdSink, MonoNs, OpKind, PxExact,
    RateCharge, RawFrame, SpecTable, StreamId, Subscription, Ticks, TimerTag, TrafficClass,
    VenueError, VenueMeta, WallNs, WireSlice,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{BOOK_LEVEL2, TOUCH_BBO};

/// Each feed the codec decodes and its wire channel, which a subscription names and each of
/// its frames carries as its `type`.
const CHANNELS: [(Feed, &str); 6] = [
    (Feed::Touch(TOUCH_BBO), "bbo"),
    (Feed::Book(BOOK_LEVEL2), "level2"),
    (Feed::Trades, "trade"),
    (Feed::Mark, "mark_price"),
    (Feed::Index, "underlier"),
    (Feed::Funding, "funding"),
];

/// The feed of a wire channel.
fn feed_of(channel: &str) -> Option<Feed> {
    CHANNELS
        .iter()
        .find(|(_, name)| *name == channel)
        .map(|(feed, _)| *feed)
}

/// Check a subscription and return its wire channel and exact venue symbol.
pub(crate) fn channel(
    specs: &SpecTable,
    sub: Subscription,
) -> Result<(&'static str, String), VenueError> {
    let name = CHANNELS
        .iter()
        .find(|(feed, _)| *feed == sub.feed)
        .map(|(_, name)| *name)
        .ok_or(VenueError::UnsupportedFeed(sub))?;
    let spec = specs
        .get(sub.inst)
        .ok_or(VenueError::UnknownInstrument(sub.inst))?;
    Ok((name, spec.venue_symbol.as_wire().to_owned()))
}

/// What the codec counted: frames it took that push no event.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct QfexMdCounts {
    /// `underlier` frames whose `source` is `internal`, each taken and pushing no
    /// [`MdEvent::Index`].
    pub internal_underliers: u64,
}

/// The codec for one connection epoch of QFEX's public MDS WebSocket.
#[derive(Clone)]
pub struct QfexMd {
    stream: StreamId,
    active: BTreeSet<Subscription>,
    epochs: BTreeMap<InstrumentId, u32>,
    counts: QfexMdCounts,
}

#[derive(Default)]
struct BufferedEvents(Vec<(VenueMeta, MdEvent)>);

impl MdSink for BufferedEvents {
    fn push(&mut self, meta: VenueMeta, event: MdEvent) {
        self.0.push((meta, event));
    }
}

impl QfexMd {
    /// A codec writing its subscriptions to `stream`.
    pub fn new(stream: StreamId) -> QfexMd {
        QfexMd {
            stream,
            active: BTreeSet::new(),
            epochs: BTreeMap::new(),
            counts: QfexMdCounts::default(),
        }
    }

    /// What it counted so far, frames refused excepted: a frame refused counts nothing.
    pub fn counts(&self) -> QfexMdCounts {
        self.counts
    }

    fn decode_item(
        &mut self,
        value: &Value,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let data = value.as_object().ok_or(DecodeError::Malformed("object"))?;
        if data.contains_key("err") {
            return self.on_error(data, specs, sink, fx);
        }
        if let Some(contents) = data.get("contents") {
            let items = contents
                .as_array()
                .ok_or(DecodeError::Malformed("contents"))?;
            for item in items {
                let mut nested = item
                    .as_object()
                    .ok_or(DecodeError::Malformed("contents"))?
                    .clone();
                if !nested.contains_key("type")
                    && let Some(kind) = data.get("type")
                {
                    nested.insert("type".to_owned(), kind.clone());
                }
                if nested.contains_key("contents") {
                    return Err(DecodeError::Malformed("contents"));
                }
                self.decode(&nested, scope, specs, sink)?;
            }
            Ok(())
        } else {
            self.decode(data, scope, specs, sink)
        }
    }

    fn send(
        &self,
        kind: &'static str,
        channels: BTreeMap<&'static str, BTreeSet<String>>,
        fx: &mut Effects,
    ) {
        for (channel, symbols) in channels {
            if symbols.is_empty() {
                continue;
            }
            let frame = json!({"type": kind, "channels": [channel], "symbols": symbols})
                .to_string()
                .into_bytes();
            fx.push(Effect::Send {
                stream: self.stream,
                frame: WireSlice::plain(frame),
                rpc: None,
                class: TrafficClass::Normal,
                charge: RateCharge::one(OpKind::Subscribe, None),
            });
        }
    }

    fn decode(
        &mut self,
        data: &Map<String, Value>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
    ) -> Result<(), DecodeError> {
        let kind = string(data, "type")?;
        let symbol = string(data, "symbol")?;
        let spec = specs
            .by_symbol(symbol)
            .ok_or(DecodeError::UnknownInstrument)?;
        let feed = feed_of(kind).ok_or(DecodeError::Malformed("type"))?;
        if !self.active.contains(&Subscription {
            inst: spec.id,
            feed,
        }) {
            return Err(DecodeError::Malformed("an unsubscribed feed"));
        }
        let meta = metadata(data)?;
        match kind {
            "bbo" => {
                let bid = one_level(spec, data, "bid")?;
                let ask = one_level(spec, data, "ask")?;
                sink.push(
                    meta,
                    MdEvent::Touch {
                        inst: spec.id,
                        bid,
                        ask,
                        source: TOUCH_BBO,
                    },
                );
            }
            "level2" => {
                // Decode both sides fully before emitting a snapshot: a malformed trailing level
                // must not leave the book halfway replaced.
                let bids = levels(spec, data, "bid", 20)?;
                let asks = levels(spec, data, "ask", 20)?;
                if bids.windows(2).any(|w| w[0].px <= w[1].px)
                    || asks.windows(2).any(|w| w[0].px >= w[1].px)
                    || bids
                        .first()
                        .zip(asks.first())
                        .is_some_and(|(b, a)| b.px >= a.px)
                {
                    return Err(DecodeError::Malformed("level2 order"));
                }
                let next = self.epochs.entry(spec.id).or_insert(0);
                *next = next.wrapping_add(1);
                let (inst, book, epoch) = (spec.id, BOOK_LEVEL2, *next);
                sink.push(meta, MdEvent::BookSnapshotBegin { inst, book, epoch });
                for (side, list) in [(BookSide::Bid, &bids), (BookSide::Ask, &asks)] {
                    for level in list {
                        sink.push(
                            meta,
                            MdEvent::Level {
                                inst,
                                book,
                                side,
                                px: level.px,
                                qty: level.qty,
                            },
                        );
                    }
                }
                if let (Some(lo), Some(hi)) = (
                    bids.iter().chain(&asks).map(|l| l.px).min(),
                    bids.iter().chain(&asks).map(|l| l.px).max(),
                ) {
                    sink.push(meta, MdEvent::Window { inst, book, lo, hi });
                }
                sink.push(meta, MdEvent::BookSnapshotEnd { inst, book });
            }
            "trade" => {
                let px = price(spec, data.get("price"), "price")?;
                let qty = quantity(spec, data.get("size"), "size")?;
                if qty == Lots::ZERO {
                    return Err(DecodeError::Malformed("size"));
                }
                match string(data, "side")?.to_ascii_uppercase().as_str() {
                    "BUY" | "SELL" => (),
                    _ => return Err(DecodeError::Malformed("side")),
                }
                let id = scope
                    .trade_id_uuid(string(data, "trade_id")?)
                    .map_err(|_| DecodeError::Malformed("trade_id"))?;
                sink.push(
                    meta,
                    MdEvent::Trade {
                        inst: spec.id,
                        id: Some(id),
                        aggressor: Aggressor::Unknown,
                        px,
                        qty,
                    },
                );
            }
            "mark_price" => {
                let px = reference_price(data)?;
                sink.push(meta, MdEvent::Mark { inst: spec.id, px });
            }
            "underlier" => {
                let px = reference_price(data)?;
                match string(data, "source")? {
                    "external" => sink.push(meta, MdEvent::Index { inst: spec.id, px }),
                    "internal" => self.counts.internal_underliers += 1,
                    _ => return Err(DecodeError::Malformed("source")),
                }
            }
            "funding" => {
                let rate_e12 = rate_e12(data)?;
                sink.push(
                    meta,
                    MdEvent::Funding {
                        inst: spec.id,
                        rate_e12,
                        interval: None,
                        next: None,
                    },
                );
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn on_error(
        &mut self,
        data: &Map<String, Value>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        // QFEX's error envelope echoes the rejected request. Only a rejected subscribe has
        // subscription meaning; the human-readable error text is never needed here.
        let err = data
            .get("err")
            .and_then(Value::as_object)
            .ok_or(DecodeError::Malformed("err"))?;
        let code = string(err, "error_code")?;
        if matches!(code, "RateLimited" | "ServerError") {
            // QFEX permits an absent request echo. A fresh epoch resubscribes through
            // the runtime's reconnect pacing, even when we cannot identify the feed.
            fx.push(Effect::Reconnect {
                stream: self.stream,
                reason: "transient QFEX subscription error",
            });
            return Ok(());
        }
        let request = match err.get("incoming_message") {
            Some(Value::Object(request)) => request,
            None | Some(Value::Null) => {
                // Without an echo, the rejected feed is unknown. A decode error alone
                // only increments a counter, leaving a settled feed silent on this epoch.
                fx.push(Effect::Reconnect {
                    stream: self.stream,
                    reason: "unattributed QFEX error",
                });
                return Ok(());
            }
            _ => return Err(DecodeError::Malformed("incoming_message")),
        };
        match string(request, "type")? {
            "unsubscribe" => {
                // Our active set already removed the feed. A rejected wire removal leaves
                // the venue sending it, so start a fresh epoch with the desired set.
                fx.push(Effect::Reconnect {
                    stream: self.stream,
                    reason: "rejected QFEX unsubscribe",
                });
                return Ok(());
            }
            "subscribe" => (),
            _ => return Err(DecodeError::Malformed("incoming_message")),
        }
        let channels = request
            .get("channels")
            .and_then(Value::as_array)
            .ok_or(DecodeError::Malformed("channels"))?;
        let symbols = request
            .get("symbols")
            .and_then(Value::as_array)
            .ok_or(DecodeError::Malformed("symbols"))?;
        let mut refused = BTreeSet::new();
        for channel in channels {
            let feed = channel
                .as_str()
                .and_then(feed_of)
                .ok_or(DecodeError::Malformed("channels"))?;
            for symbol in symbols {
                let wire = symbol.as_str().ok_or(DecodeError::Malformed("symbols"))?;
                let spec = specs
                    .by_symbol(wire)
                    .ok_or(DecodeError::UnknownInstrument)?;
                refused.insert(Subscription {
                    inst: spec.id,
                    feed,
                });
            }
        }
        if refused.is_empty() || refused.iter().any(|sub| !self.active.contains(sub)) {
            return Err(DecodeError::Malformed("incoming_message"));
        }
        for sub in refused {
            self.active.remove(&sub);
            sink.push(
                VenueMeta::NONE,
                MdEvent::Health {
                    inst: sub.inst,
                    feed: sub.feed,
                    h: FeedHealth::Refused,
                },
            );
        }
        Ok(())
    }
}

impl MdCodec for QfexMd {
    fn on_open(&mut self, _fx: &mut Effects) {}

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        let mut adds: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        let mut removes: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        for &sub in add {
            let (channel, symbol) = channel(specs, sub)?;
            if !self.active.contains(&sub) {
                adds.entry(channel).or_default().insert(symbol);
            }
        }
        for &sub in remove {
            let (channel, symbol) = channel(specs, sub)?;
            if self.active.contains(&sub) {
                removes.entry(channel).or_default().insert(symbol);
            }
        }
        // Validate all subscriptions before changing state or sending a frame.
        self.send("subscribe", adds, fx);
        self.send("unsubscribe", removes, fx);
        self.active.extend(add.iter().copied());
        for sub in remove {
            self.active.remove(sub);
        }
        Ok(())
    }

    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let RawFrame::Text(text) = f else {
            return Err(DecodeError::Malformed("binary frame"));
        };
        let value: Value =
            serde_json::from_str(text).map_err(|_| DecodeError::Malformed("JSON"))?;
        if value
            .as_object()
            .is_some_and(|data| !data.contains_key("contents"))
        {
            return self.decode_item(&value, scope, specs, sink, fx);
        }
        // A malformed member invalidates the whole frame, including earlier members.
        let mut staged = self.clone();
        let mut events = BufferedEvents::default();
        let mut effects = Effects::new();
        let result = match &value {
            Value::Array(items) => items.iter().try_for_each(|item| {
                staged.decode_item(item, scope, specs, &mut events, &mut effects)
            }),
            _ => staged.decode_item(&value, scope, specs, &mut events, &mut effects),
        };
        result?;
        for effect in effects.take() {
            fx.push(effect);
        }
        *self = staged;
        for (meta, event) in events.0 {
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
        Err(DecodeError::Malformed("unexpected HTTP response"))
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

    fn keepalive(&self) -> Option<Keepalive> {
        None
    }

    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

fn string<'a>(data: &'a Map<String, Value>, key: &'static str) -> Result<&'a str, DecodeError> {
    data.get(key)
        .and_then(Value::as_str)
        .ok_or(DecodeError::Malformed(key))
}

fn metadata(data: &Map<String, Value>) -> Result<VenueMeta, DecodeError> {
    let time = OffsetDateTime::parse(string(data, "time")?, &Rfc3339)
        .map_err(|_| DecodeError::Malformed("time"))?;
    let ns =
        i64::try_from(time.unix_timestamp_nanos()).map_err(|_| DecodeError::Malformed("time"))?;
    let seq = data
        .get("sequence")
        .map(|v| v.as_u64().ok_or(DecodeError::Malformed("sequence")))
        .transpose()?;
    Ok(VenueMeta {
        exch_ts: Some(ExchNs(ns)),
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: seq,
    })
}

fn price(
    spec: &InstrumentSpec,
    value: Option<&Value>,
    key: &'static str,
) -> Result<Ticks, DecodeError> {
    let bad = DecodeError::Malformed(key);
    let px: PxExact = value
        .and_then(Value::as_str)
        .ok_or(bad)?
        .parse()
        .map_err(|_| bad)?;
    spec.price_grid.ticks_exact(px).ok_or(bad)
}

/// A mark or underlier `price`: a positive decimal string, exact.
fn reference_price(data: &Map<String, Value>) -> Result<PxExact, DecodeError> {
    let bad = DecodeError::Malformed("price");
    let px: PxExact = string(data, "price")?.parse().map_err(|_| bad)?;
    if px.mantissa <= 0 {
        return Err(bad);
    }
    Ok(px)
}

/// A `funding` frame's `funding_rate`, a decimal fraction as a decimal string, in units of
/// 1e-12 exactly: refused where it is finer than that or outside `i64`, never rounded.
fn rate_e12(data: &Map<String, Value>) -> Result<i64, DecodeError> {
    let bad = DecodeError::Malformed("funding_rate");
    let rate: PxExact = string(data, "funding_rate")?.parse().map_err(|_| bad)?;
    // rate = mantissa x 10^exp, so rate x 10^12 = mantissa x 10^(exp + 12).
    let shift = i32::from(rate.exp) + 12;
    let scale = 10_i64.checked_pow(shift.unsigned_abs()).ok_or(bad);
    if shift >= 0 {
        rate.mantissa.checked_mul(scale?).ok_or(bad)
    } else {
        match scale {
            Ok(scale) if rate.mantissa % scale == 0 => Ok(rate.mantissa / scale),
            // A divisor beyond i64 leaves a whole number of 1e-12 only for zero.
            Err(_) if rate.mantissa == 0 => Ok(0),
            _ => Err(bad),
        }
    }
}

fn quantity(
    spec: &InstrumentSpec,
    value: Option<&Value>,
    key: &'static str,
) -> Result<Lots, DecodeError> {
    let bad = DecodeError::Malformed(key);
    let qty =
        Decimal::from_str_exact(value.and_then(Value::as_str).ok_or(bad)?).map_err(|_| bad)?;
    let step = spec.size_step.get();
    let lots = qty.checked_div(step).ok_or(bad)?;
    if !lots.fract().is_zero() || lots.checked_mul(step) != Some(qty) {
        return Err(bad);
    }
    lots.to_i64().and_then(Lots::new).ok_or(bad)
}

fn levels(
    spec: &InstrumentSpec,
    data: &Map<String, Value>,
    key: &'static str,
    max: usize,
) -> Result<Vec<Lvl>, DecodeError> {
    let bad = DecodeError::Malformed(key);
    let array = data.get(key).and_then(Value::as_array).ok_or(bad)?;
    if array.len() > max {
        return Err(bad);
    }
    array
        .iter()
        .map(|item| {
            let pair = item.as_array().ok_or(bad)?;
            let [p, q] = pair.as_slice() else {
                return Err(bad);
            };
            let px = price(spec, Some(p), key)?;
            let qty = quantity(spec, Some(q), key)?;
            if qty == Lots::ZERO {
                return Err(bad);
            }
            Ok(Lvl { px, qty })
        })
        .collect()
}

fn one_level(
    spec: &InstrumentSpec,
    data: &Map<String, Value>,
    key: &'static str,
) -> Result<Option<Lvl>, DecodeError> {
    Ok(levels(spec, data, key, 1)?.into_iter().next())
}
