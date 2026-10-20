//! Sans-IO decoder for QFEX's public MDS WebSocket.

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

/// Check a subscription and return its wire channel and exact venue symbol.
pub(crate) fn channel(
    specs: &SpecTable,
    sub: Subscription,
) -> Result<(&'static str, String), VenueError> {
    let name = match sub.feed {
        Feed::Touch(TOUCH_BBO) => "bbo",
        Feed::Book(BOOK_LEVEL2) => "level2",
        Feed::Trades => "trade",
        _ => return Err(VenueError::UnsupportedFeed(sub)),
    };
    let spec = specs
        .get(sub.inst)
        .ok_or(VenueError::UnknownInstrument(sub.inst))?;
    Ok((name, spec.venue_symbol.as_wire().to_owned()))
}

pub(crate) struct QfexMd {
    stream: StreamId,
    active: BTreeSet<Subscription>,
    epochs: BTreeMap<InstrumentId, u32>,
}

impl QfexMd {
    pub(crate) fn new(stream: StreamId) -> Self {
        Self {
            stream,
            active: BTreeSet::new(),
            epochs: BTreeMap::new(),
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
        specs: &SpecTable,
        sink: &mut dyn MdSink,
    ) -> Result<(), DecodeError> {
        let kind = string(data, "type")?;
        let symbol = string(data, "symbol")?;
        let spec = specs
            .by_symbol(symbol)
            .ok_or(DecodeError::UnknownInstrument)?;
        let feed = match kind {
            "bbo" => Feed::Touch(TOUCH_BBO),
            "level2" => Feed::Book(BOOK_LEVEL2),
            "trade" => Feed::Trades,
            _ => return Err(DecodeError::Malformed("type")),
        };
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
                // The documented trade_id is a UUID; the normalized event accepts u64 only.
                sink.push(
                    meta,
                    MdEvent::Trade {
                        inst: spec.id,
                        id: None,
                        aggressor: Aggressor::Unknown,
                        px,
                        qty,
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
        let request = err
            .get("incoming_message")
            .and_then(Value::as_object)
            .ok_or(DecodeError::Malformed("incoming_message"))?;
        if string(request, "type")? != "subscribe" {
            return Err(DecodeError::Malformed("incoming_message"));
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
            let feed = match channel.as_str() {
                Some("bbo") => Feed::Touch(TOUCH_BBO),
                Some("level2") => Feed::Book(BOOK_LEVEL2),
                Some("trade") => Feed::Trades,
                _ => return Err(DecodeError::Malformed("channels")),
            };
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
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let RawFrame::Text(text) = f else {
            return Err(DecodeError::Malformed("binary frame"));
        };
        let value: Value =
            serde_json::from_str(text).map_err(|_| DecodeError::Malformed("JSON"))?;
        let data = value.as_object().ok_or(DecodeError::Malformed("object"))?;
        if data.contains_key("err") {
            self.on_error(data, specs, sink, fx)
        } else {
            self.decode(data, specs, sink)
        }
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

fn quantity(
    spec: &InstrumentSpec,
    value: Option<&Value>,
    key: &'static str,
) -> Result<Lots, DecodeError> {
    let bad = DecodeError::Malformed(key);
    let qty =
        Decimal::from_str_exact(value.and_then(Value::as_str).ok_or(bad)?).map_err(|_| bad)?;
    let lots = qty.checked_div(spec.size_step.get()).ok_or(bad)?;
    if !lots.fract().is_zero() {
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
