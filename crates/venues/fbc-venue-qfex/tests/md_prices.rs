//! QFEX's `mark_price`, `underlier` and `funding` channels: the documented examples, the
//! 2026-11-04 public capture under `fixtures/qfex/live/`, and what the capture settles about the
//! funding rate's unit and `time_remaining` (see `fixtures/qfex/README.md`).

use std::collections::BTreeSet;

use fbc_core::{
    AssetSym, DecodeError, Effect, Effects, ExchNs, ExchTsKind, Feed, FeedHealth, FeedSource,
    FundingCaps, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec, Lots, MdCodec, MdEvent,
    MdSink, PriceGrid, PxExact, RawFrame, SizeStep, SpecTable, StreamId, Subscription,
    TradingStatus, UnderlyingId, VenueConfig, VenueFactory, VenueId, VenueMeta, WallNs,
    dispatch_market_data,
};
use fbc_venue_qfex::{KEY_MDS_URL, Qfex, QfexMd, QfexMdCounts};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const INST: InstrumentId = InstrumentId::new(1);
const PRICES: [Feed; 3] = [Feed::Mark, Feed::Index, Feed::Funding];

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg
}

fn specs_for(symbol: &str) -> SpecTable {
    let caps = Qfex.caps(&cfg()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol(symbol)).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    let mut specs = SpecTable::new();
    specs.insert(InstrumentSpec {
        id: INST,
        venue: VenueId::new(4),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed("0.01".parse().unwrap()).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new("0.01".parse::<Decimal>().unwrap()).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
        version: 1,
        fetched_at: WallNs(0),
    });
    specs
}

fn sub(feed: Feed) -> Subscription {
    Subscription { inst: INST, feed }
}

#[derive(Default)]
struct Sink(Vec<(VenueMeta, MdEvent)>);
impl MdSink for Sink {
    fn push(&mut self, meta: VenueMeta, event: MdEvent) {
        self.0.push((meta, event));
    }
}

/// A codec with `feeds` of the one instrument subscribed.
fn codec(specs: &SpecTable, feeds: &[Feed]) -> QfexMd {
    let subs: Vec<_> = feeds.iter().copied().map(sub).collect();
    let mut codec = QfexMd::new(StreamId(0));
    codec
        .subscribe(&subs, &[], specs, &mut Effects::new())
        .unwrap();
    codec
}

fn decode(codec: &mut QfexMd, frame: &str, specs: &SpecTable) -> (Result<(), DecodeError>, Sink) {
    let mut sink = Sink::default();
    let caps = Qfex.caps(&cfg()).unwrap();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Text(frame),
            scope,
            specs,
            &mut sink,
            &mut Effects::new(),
        )
    });
    (result, sink)
}

fn frame(name: &str) -> Value {
    serde_json::from_str(match name {
        "mark_price" => include_str!("../../../../fixtures/qfex/md/mark_price.json"),
        "underlier" => include_str!("../../../../fixtures/qfex/md/underlier.json"),
        "funding" => include_str!("../../../../fixtures/qfex/md/funding.json"),
        _ => unreachable!(),
    })
    .unwrap()
}

fn nanos(time: &str) -> i128 {
    OffsetDateTime::parse(time, &Rfc3339)
        .unwrap()
        .unix_timestamp_nanos()
}

fn meta(time: &str, seq: Option<u64>) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(i64::try_from(nanos(time)).unwrap())),
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: seq,
    }
}

/// The rate in units of 1e-12, by `rust_decimal`, independently of the codec.
fn e12(rate: &str) -> i64 {
    let exact = Decimal::from_str_exact(rate).unwrap() * Decimal::from(1_000_000_000_000_i64);
    assert!(exact.fract().is_zero(), "{rate}");
    exact.to_i64().unwrap()
}

#[test]
fn documented_mark_underlier_and_funding_frames_decode_exactly() {
    let specs = specs_for("AAPL-USD");
    let mut codec = codec(&specs, &PRICES);
    // The mark_price page's example has no sequence.
    let (result, sink) = decode(&mut codec, &frame("mark_price").to_string(), &specs);
    result.unwrap();
    assert_eq!(
        sink.0,
        [(
            meta("2025-05-08T20:52:48.000Z", None),
            MdEvent::Mark {
                inst: INST,
                px: PxExact::new(43_745, -2),
            }
        )]
    );
    let (result, sink) = decode(&mut codec, &frame("underlier").to_string(), &specs);
    result.unwrap();
    assert_eq!(
        sink.0,
        [(
            meta("2014-11-07T08:19:27.028459Z", Some(50)),
            MdEvent::Index {
                inst: INST,
                px: PxExact::new(40_023, -2),
            }
        )]
    );
    // "0.021" is a decimal fraction (the funding page, market_stats, and the capture): 0.021 x
    // 1e12. No frame states the interval, and time + time_remaining is not the settlement time.
    let (result, sink) = decode(&mut codec, &frame("funding").to_string(), &specs);
    result.unwrap();
    assert_eq!(
        sink.0,
        [(
            meta("2014-11-07T08:19:27.028459Z", Some(50)),
            MdEvent::Funding {
                inst: INST,
                rate_e12: 21_000_000_000,
                interval: None,
                next: None,
            }
        )]
    );
    assert_eq!(codec.counts(), QfexMdCounts::default());
}

#[test]
fn an_internal_underlier_emits_no_event_and_is_counted() {
    let specs = specs_for("AAPL-USD");
    let mut codec = codec(&specs, &PRICES);
    let mut internal = frame("underlier");
    internal["source"] = json!("internal");
    let (result, sink) = decode(&mut codec, &internal.to_string(), &specs);
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty());
    assert_eq!(codec.counts().internal_underliers, 1);
    // In a batch, the external one decodes and the internal one is counted.
    let batch = json!([internal, frame("underlier"), internal]).to_string();
    let (result, sink) = decode(&mut codec, &batch, &specs);
    assert_eq!(result, Ok(()));
    assert_eq!(sink.0.len(), 1);
    assert!(matches!(sink.0[0].1, MdEvent::Index { .. }));
    assert_eq!(codec.counts().internal_underliers, 3);
    // A batch refused as a whole counts nothing.
    let mut bad = frame("funding");
    bad["funding_rate"] = json!("x");
    let (result, sink) = decode(&mut codec, &json!([internal, bad]).to_string(), &specs);
    assert_eq!(result, Err(DecodeError::Malformed("funding_rate")));
    assert!(sink.0.is_empty());
    assert_eq!(codec.counts().internal_underliers, 3);
    // An internal frame's price is still checked before it is counted.
    let mut unreadable = internal.clone();
    unreadable["price"] = json!("0");
    let (result, _) = decode(&mut codec, &unreadable.to_string(), &specs);
    assert_eq!(result, Err(DecodeError::Malformed("price")));
    assert_eq!(codec.counts().internal_underliers, 3);
    // An internal underlier of a feed not subscribed is refused, not counted.
    let mut marks = codec_without_index(&specs);
    let (result, _) = decode(&mut marks, &internal.to_string(), &specs);
    assert_eq!(result, Err(DecodeError::Malformed("an unsubscribed feed")));
    assert_eq!(marks.counts().internal_underliers, 0);
}

fn codec_without_index(specs: &SpecTable) -> QfexMd {
    codec(specs, &[Feed::Mark, Feed::Funding])
}

#[test]
fn a_malformed_or_out_of_range_number_refuses_the_whole_frame() {
    let specs = specs_for("AAPL-USD");
    let mut codec = codec(&specs, &PRICES);
    let accepted = [
        ("-0.00000", 0),
        ("0.0000000000010", 1),
        ("-0.00010", -100_000_000),
        ("9223372.036854775807", i64::MAX),
        ("-9223372.036854775808", i64::MIN),
    ];
    for (rate, rate_e12) in accepted {
        let mut funding = frame("funding");
        funding["funding_rate"] = json!(rate);
        let (result, sink) = decode(&mut codec, &funding.to_string(), &specs);
        assert_eq!(result, Ok(()), "{rate}");
        assert!(
            matches!(sink.0[..], [(_, MdEvent::Funding { rate_e12: got, .. })] if got == rate_e12),
            "{rate}"
        );
    }
    let mut cases = Vec::new();
    for rate in [
        json!("0.0000000000001"),
        json!("-0.0000000000015"),
        json!("9223372.036854775808"),
        json!("-9223372.036854775809"),
        json!("10000000"),
        json!("1e-5"),
        json!("+0.1"),
        json!(""),
        json!("."),
        json!("0.0.1"),
        json!(" 0.1"),
        json!(0.021),
        Value::Null,
    ] {
        let mut bad = frame("funding");
        bad["funding_rate"] = rate;
        cases.push(bad);
    }
    let mut bad = frame("funding");
    bad.as_object_mut().unwrap().remove("funding_rate");
    cases.push(bad);
    for name in ["mark_price", "underlier"] {
        for price in [
            json!("0"),
            json!("-437.45"),
            json!("abc"),
            json!("1e3"),
            json!("99999999999999999999"),
            json!(437.45),
            Value::Null,
        ] {
            let mut bad = frame(name);
            bad["price"] = price;
            cases.push(bad);
        }
        let mut bad = frame(name);
        bad.as_object_mut().unwrap().remove("price");
        cases.push(bad);
        let mut bad = frame(name);
        bad["time"] = json!("not a time");
        cases.push(bad);
        let mut bad = frame(name);
        bad["sequence"] = json!(-1);
        cases.push(bad);
    }
    for source in [json!("other"), json!("EXTERNAL"), json!(1), Value::Null] {
        let mut bad = frame("underlier");
        bad["source"] = source;
        cases.push(bad);
    }
    let mut bad = frame("underlier");
    bad.as_object_mut().unwrap().remove("source");
    cases.push(bad);
    for case in cases {
        // Alone, and after a valid member of the same frame: nothing is pushed either way.
        for raw in [
            case.to_string(),
            json!([frame("mark_price"), case]).to_string(),
        ] {
            let (result, sink) = decode(&mut codec, &raw, &specs);
            assert!(result.is_err(), "{raw}");
            assert!(sink.0.is_empty(), "{raw}");
        }
    }
    assert_eq!(codec.counts(), QfexMdCounts::default());
}

#[test]
fn subscriptions_name_mark_price_underlier_and_funding() {
    let specs = specs_for("AAPL-USD");
    let subs: BTreeSet<_> = PRICES.map(sub).into_iter().collect();
    let plan = Qfex.plan_md(&cfg(), &specs, &subs).unwrap().remove(0);
    let mut wire = Qfex.md_codec(&cfg(), &plan);
    // Each frame as canonical JSON text, keys sorted.
    let sent = |fx: &Effects| -> BTreeSet<String> {
        fx.as_slice()
            .iter()
            .map(|effect| match effect {
                Effect::Send { frame, .. } => serde_json::from_slice::<Value>(frame.bytes())
                    .unwrap()
                    .to_string(),
                _ => panic!("not a subscription frame"),
            })
            .collect()
    };
    let frames = |kind: &str| -> BTreeSet<String> {
        ["mark_price", "underlier", "funding"]
            .map(|channel| {
                json!({"type": kind, "channels": [channel], "symbols": ["AAPL-USD"]}).to_string()
            })
            .into_iter()
            .collect()
    };
    let mut fx = Effects::new();
    wire.subscribe(&plan.subs, &[], &specs, &mut fx).unwrap();
    assert_eq!(sent(&fx), frames("subscribe"));
    let mut fx = Effects::new();
    wire.subscribe(&[], &plan.subs, &specs, &mut fx).unwrap();
    assert_eq!(sent(&fx), frames("unsubscribe"));
    // Once removed, each of the three is refused as unsubscribed.
    for name in ["mark_price", "underlier", "funding"] {
        let mut sink = Sink::default();
        let caps = Qfex.caps(&cfg()).unwrap();
        let result = dispatch_market_data(&caps, |scope| {
            wire.on_frame(
                RawFrame::Text(&frame(name).to_string()),
                scope,
                &specs,
                &mut sink,
                &mut Effects::new(),
            )
        });
        assert_eq!(result, Err(DecodeError::Malformed("an unsubscribed feed")));
        assert!(sink.0.is_empty());
    }
    // A refused subscription of each reports that feed refused.
    for (channel, feed) in [
        ("mark_price", Feed::Mark),
        ("underlier", Feed::Index),
        ("funding", Feed::Funding),
    ] {
        let mut codec = codec(&specs, &PRICES);
        let error = json!({"err": {"error_code": "InvalidParameter", "incoming_message":
            {"type": "subscribe", "channels": [channel], "symbols": ["AAPL-USD"]}}});
        let (result, sink) = decode(&mut codec, &error.to_string(), &specs);
        assert_eq!(result, Ok(()));
        assert_eq!(
            sink.0,
            [(
                VenueMeta::NONE,
                MdEvent::Health {
                    inst: INST,
                    feed,
                    h: FeedHealth::Refused,
                }
            )]
        );
        let (result, _) = decode(&mut codec, &frame(channel).to_string(), &specs);
        assert_eq!(result, Err(DecodeError::Malformed("an unsubscribed feed")));
    }
}

#[test]
fn caps_declare_mark_index_and_funding_streams_without_interval_or_next_time() {
    let caps = Qfex.caps(&cfg()).unwrap();
    assert_eq!(caps.md.mark, FeedSource::Stream);
    assert_eq!(caps.md.index, FeedSource::Stream);
    assert_eq!(
        caps.md.funding,
        FundingCaps {
            source: FeedSource::Stream,
            interval_reported: false,
            next_time_reported: false,
        }
    );
    assert_eq!(caps.md.stats, FeedSource::None);
}

/// What the capture's funding frames and market_stats entries say about one funding state.
struct FundingSeen {
    time: String,
    rate: String,
    annualised: String,
    remaining: i64,
}

#[test]
fn the_public_capture_decodes_exactly_and_settles_the_funding_unit() {
    let projected: Vec<Value> =
        include_str!("../../../../fixtures/qfex/live/suig-2026-11-04.ndjson")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let recorded: Vec<Value> =
        include_str!("../../../../fixtures/qfex/live/suig-2026-11-04.raw.ndjson")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    assert_eq!(projected.len(), recorded.len());
    for (projection, saved) in projected.iter().zip(&recorded) {
        assert_eq!(projection["recv_ms"], saved["recv_ms"]);
        let raw: Value = serde_json::from_str(saved["raw"].as_str().unwrap()).unwrap();
        let mut expected = projection.clone();
        expected.as_object_mut().unwrap().remove("recv_ms");
        assert_eq!(raw, expected, "the projection keeps the raw frame exactly");
    }

    let specs = specs_for("SUIG-USD");
    let mut codec = codec(&specs, &PRICES);
    let (mut marks, mut external, mut internal) = (0, 0, 0);
    let mut seen = Vec::new();
    // Each funding frame's receipt (milliseconds after the capture started), time and
    // time_remaining.
    let mut pulses = Vec::new();
    for (index, frame) in projected.iter().enumerate() {
        let kind = frame["type"].as_str().unwrap();
        let seq = frame["sequence"].as_u64().unwrap();
        // One connection's sequence, across the four channels.
        assert_eq!(seq, index as u64 + 1);
        let time = frame["time"].as_str().unwrap();
        if kind == "market_stats" {
            for entry in frame["funding_rates"].as_array().unwrap() {
                seen.push(FundingSeen {
                    time: entry["time"].as_str().unwrap().to_owned(),
                    rate: entry["funding_rate"].as_str().unwrap().to_owned(),
                    annualised: entry["annualised_funding_rate"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    remaining: entry["time_remaining"].as_i64().unwrap(),
                });
            }
            continue;
        }
        let raw = recorded[index]["raw"].as_str().unwrap();
        let (result, sink) = decode(&mut codec, raw, &specs);
        result.unwrap();
        let at = meta(time, Some(seq));
        let price = || frame["price"].as_str().unwrap().parse::<PxExact>().unwrap();
        match kind {
            "mark_price" => {
                marks += 1;
                let mark = MdEvent::Mark {
                    inst: INST,
                    px: price(),
                };
                assert_eq!(sink.0, [(at, mark)]);
            }
            "underlier" => match frame["source"].as_str().unwrap() {
                "external" => {
                    external += 1;
                    let index = MdEvent::Index {
                        inst: INST,
                        px: price(),
                    };
                    assert_eq!(sink.0, [(at, index)]);
                }
                "internal" => {
                    internal += 1;
                    assert!(sink.0.is_empty());
                }
                other => panic!("unexpected source {other}"),
            },
            "funding" => {
                let rate = frame["funding_rate"].as_str().unwrap();
                let funding = MdEvent::Funding {
                    inst: INST,
                    rate_e12: e12(rate),
                    interval: None,
                    next: None,
                };
                assert_eq!(sink.0, [(at, funding)]);
                pulses.push((
                    frame["recv_ms"].as_f64().unwrap(),
                    nanos(time),
                    frame["time_remaining"].as_i64().unwrap(),
                ));
                seen.push(FundingSeen {
                    time: time.to_owned(),
                    rate: rate.to_owned(),
                    annualised: frame["annualised_funding_rate"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    remaining: frame["time_remaining"].as_i64().unwrap(),
                });
            }
            other => panic!("unexpected captured feed {other}"),
        }
    }
    assert_eq!((marks, external, internal), (60, 1, 26));
    assert_eq!(
        codec.counts(),
        QfexMdCounts {
            internal_underliers: internal,
        }
    );
    // 3 funding frames, and 60 market_stats entries repeating those 3 states.
    assert_eq!(seen.len(), 63);
    let states: BTreeSet<_> = seen.iter().map(|s| s.time.clone()).collect();
    assert_eq!(states.len(), 3);

    // The unit. Every rate is written to exactly five decimal places, the annualised rate to
    // four, so each bounds the factor annualised / rate by its half-unit rounding.
    let (mut lo, mut hi) = (f64::MIN, f64::MAX);
    for s in &seen {
        assert_eq!(s.rate.split_once('.').unwrap().1.len(), 5, "{}", s.rate);
        assert_eq!(s.annualised.split_once('.').unwrap().1.len(), 4);
        let rate: f64 = s.rate.parse().unwrap();
        let annualised: f64 = s.annualised.parse().unwrap();
        assert!(rate != 0.0 && rate.signum() == annualised.signum());
        // Not the rate x 8760: -0.00010 x 8760 is -0.876, against -0.634 here.
        assert_ne!(Decimal::from_str_exact(&s.annualised).unwrap(), {
            Decimal::from_str_exact(&s.rate).unwrap() * Decimal::from(8760)
        });
        lo = lo.max((annualised.abs() - 0.00005) / (rate.abs() + 0.000005));
        hi = hi.min((annualised.abs() + 0.00005) / (rate.abs() - 0.000005));
    }
    // Between 6,042 and 6,680: more than the 1,095 eight-hour periods of a year and fewer than
    // its 8,760 hours, so the rate is a decimal fraction per hour annualised over fewer hours
    // than the year holds (funding accrues only in the symbol's sessions), and both are in one
    // unit: a percent rate annualised as a fraction would give at most 87.6.
    assert!(lo > 6_042.0 && hi < 6_669.0 && lo < hi, "{lo}..{hi}");
    assert!(lo > 8760.0 / 8.0 && hi < 8760.0);

    // time + time_remaining never lands on the settlement, the top of the next UTC hour: it
    // falls 1 to 10 seconds short of it (4.4 here).
    const SECOND: i128 = 1_000_000_000;
    let hour = 3_600 * SECOND;
    let boundary = |at: i128| (at / hour + 1) * hour;
    for s in &seen {
        let landed = nanos(&s.time) + i128::from(s.remaining) * SECOND;
        let short = boundary(nanos(&s.time)) - landed;
        assert!(
            short > SECOND && short < 10 * SECOND,
            "{} + {}",
            s.time,
            s.remaining
        );
    }
    // Three funding frames 30 seconds apart: the last pulse before the capture, replayed on
    // subscribing, then two pulses as they were sent.
    assert_eq!(pulses.len(), 3);
    for pair in pulses.windows(2) {
        assert!((pair[1].1 - pair[0].1 - 30 * SECOND).abs() < SECOND / 100);
    }
    // The capture started at this instant (the script's summary, recorded in
    // fixtures/qfex/README.md); recv_ms counts from it. For a pulse received as it was sent,
    // receipt plus time_remaining reaches the hour within a second, so time_remaining counts
    // from the send, not from `time`. The replayed one arrived 12 seconds after its `time`
    // with its time_remaining unchanged, and overshoots.
    let start = nanos("2026-11-04T18:06:28.920Z");
    for (index, &(ms, time, remaining)) in pulses.iter().enumerate() {
        let received = start + (ms * 1e6) as i128;
        let reached = received + i128::from(remaining) * SECOND - boundary(time);
        if index == 0 {
            assert!(received - time > 12 * SECOND && reached > 7 * SECOND);
        } else {
            assert!((0..SECOND).contains(&reached), "{reached}");
        }
    }
    let caps = Qfex.caps(&cfg()).unwrap();
    assert!(!caps.md.funding.next_time_reported);
}
