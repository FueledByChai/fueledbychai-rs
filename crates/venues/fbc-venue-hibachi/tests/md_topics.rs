//! FBC-2kmp's done line, the other topics and the subscriptions: the codec subscribes on
//! `/ws/market` with the `parameters` key, each orderbook naming its granularity and depth; it
//! decodes `ask_bid_price` into a Touch only from a fully sized frame and never leaves a stale one
//! in force, both `trades` shapes, `mark_price`, `spot_price` and both `funding_rate_estimation`
//! shapes, synthesizing no exchange timestamp, from the hand-built frames of
//! `fixtures/hibachi/md/`.

mod common;

use fbc_core::{
    Aggressor, ConfigError, DecodeError, Effect, Effects, ExchNs, ExchTsKind, Feed, FeedHealth,
    HttpFailure, HttpTag, Inbound, InboundSpans, Lots, MdCodec, MdEvent, OpKind, PxExact,
    RateCharge, RawFrame, StreamId, Ticks, TimerTag, TrafficClass, VenueError, VenueFactory,
    VenueMeta, dispatch_market_data,
};
use fbc_venue_hibachi::{Hibachi, HibachiMd, KEY_ORDERBOOK_RESUBSCRIBE, TOUCH_ASK_BID_PRICE};

use common::{
    Driver, ETH, Kept, SEI, SOL, book_sub, config, every_feed, frame, specs, sub, touch_sub,
};

fn sends(effects: &[Effect]) -> Vec<(String, RateCharge)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send {
                stream,
                frame,
                rpc,
                class,
                charge,
            } => {
                assert_eq!(
                    (*stream, *rpc, *class),
                    (StreamId(0), None, TrafficClass::Normal)
                );
                Some((String::from_utf8(frame.bytes().to_vec()).unwrap(), *charge))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn subscriptions_use_the_parameters_key_and_each_orderbook_names_its_granularity_and_depth() {
    let mut subs = every_feed(ETH);
    subs.push(book_sub(SOL));
    subs.push(book_sub(SEI));
    let mut d = Driver::new(&subs);
    let effects = d.take_fx();
    let one = |inst| RateCharge::one(OpKind::Subscribe, Some(inst));
    let frame = |body: &str| {
        format!(r#"{{"method":"subscribe","parameters":{{"subscriptions":[{body}]}}}}"#)
    };
    assert_eq!(
        sends(&effects),
        [
            (
                frame(
                    r#"{"depth":3,"granularity":"0.01","symbol":"ETH/USDT-P","topic":"orderbook"}"#
                ),
                one(ETH)
            ),
            (
                frame(r#"{"symbol":"ETH/USDT-P","topic":"ask_bid_price"}"#),
                one(ETH)
            ),
            (
                frame(r#"{"symbol":"ETH/USDT-P","topic":"trades"}"#),
                one(ETH)
            ),
            (
                frame(r#"{"symbol":"ETH/USDT-P","topic":"mark_price"}"#),
                one(ETH)
            ),
            (
                frame(r#"{"symbol":"ETH/USDT-P","topic":"spot_price"}"#),
                one(ETH)
            ),
            (
                frame(r#"{"symbol":"ETH/USDT-P","topic":"funding_rate_estimation"}"#),
                one(ETH)
            ),
            // SOL lists its own 0.001 tick: subscribed on it.
            (
                frame(
                    r#"{"depth":3,"granularity":"0.001","symbol":"SOL/USDT-P","topic":"orderbook"}"#
                ),
                one(SOL)
            ),
            // SEI lists no granularity at or above its 0.01 tick: subscribed on the tick.
            (
                frame(
                    r#"{"depth":3,"granularity":"0.01","symbol":"SEI/USDT-P","topic":"orderbook"}"#
                ),
                one(SEI)
            ),
        ]
    );
    // Each orderbook waits for its Snapshot for the interval; nothing else is asked for.
    let timers = effects
        .iter()
        .filter(|e| matches!(e, Effect::Timer { after, .. } if *after == common::INTERVAL));
    assert_eq!(timers.count(), 3);
    assert_eq!(effects.len(), 11);
    // The same subscription again sends nothing; one not taken, removed, sends nothing.
    let mut fx = Effects::new();
    let specs = specs();
    d.codec
        .subscribe(
            &[book_sub(ETH)],
            &[book_sub(fbc_core::InstrumentId::new(9))],
            &specs,
            &mut fx,
        )
        .unwrap_err();
    d.codec
        .subscribe(&[book_sub(ETH)], &[sub(SOL, Feed::Trades)], &specs, &mut fx)
        .unwrap();
    assert!(fx.is_empty());
    // A removal: one unsubscribe frame, without the parameters.
    d.codec
        .subscribe(&[], &[sub(ETH, Feed::Trades)], &specs, &mut fx)
        .unwrap();
    assert_eq!(
        sends(fx.as_slice()),
        [(
            r#"{"method":"unsubscribe","parameters":{"subscriptions":[{"symbol":"ETH/USDT-P","topic":"trades"}]}}"#
                .to_owned(),
            one(ETH)
        )]
    );
}

#[test]
fn a_subscription_is_checked_before_anything_is_sent() {
    let specs = specs();
    let mut codec = HibachiMd::new(StreamId(0), &config());
    let mut fx = Effects::new();
    for feed in [
        Feed::Stats,
        Feed::Book(fbc_core::BookId(1)),
        Feed::Touch(fbc_core::TouchSourceId(1)),
    ] {
        let s = sub(ETH, feed);
        assert_eq!(
            codec.subscribe(&[book_sub(ETH), s], &[], &specs, &mut fx),
            Err(VenueError::UnsupportedFeed(s))
        );
    }
    let missing = fbc_core::InstrumentId::new(9);
    assert_eq!(
        codec.subscribe(&[book_sub(ETH), book_sub(missing)], &[], &specs, &mut fx),
        Err(VenueError::UnknownInstrument(missing))
    );
    assert!(fx.is_empty());
    // A configuration the factory refuses refuses every subscription.
    let mut cfg = config();
    cfg.insert(KEY_ORDERBOOK_RESUBSCRIBE, "0ms");
    let mut codec = HibachiMd::new(StreamId(0), &cfg);
    assert_eq!(
        codec.subscribe(&[book_sub(ETH)], &[], &specs, &mut fx),
        Err(VenueError::Config(ConfigError::Invalid {
            key: KEY_ORDERBOOK_RESUBSCRIBE,
            reason: "not a positive whole number of milliseconds (<n>ms)",
        }))
    );
    assert!(fx.is_empty());
}

fn touch(bid: (i64, i64), ask: (i64, i64)) -> MdEvent {
    let lvl = |(px, qty): (i64, i64)| fbc_core::Lvl {
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    };
    MdEvent::Touch {
        inst: ETH,
        bid: Some(lvl(bid)),
        ask: Some(lvl(ask)),
        source: TOUCH_ASK_BID_PRICE,
    }
}

fn touch_gap() -> MdEvent {
    MdEvent::Health {
        inst: ETH,
        feed: Feed::Touch(TOUCH_ASK_BID_PRICE),
        h: FeedHealth::Gap,
    }
}

/// `abp_sized`'s Touch: bid 2000.012345 x 1.25, ask 2000.104321 x 1.5.
fn sized() -> MdEvent {
    touch(
        (2_000_012_345, 1_250_000_000),
        (2_000_104_321, 1_500_000_000),
    )
}

#[test]
fn ask_bid_price_becomes_a_touch_only_when_it_carries_each_sides_price_and_size() {
    let mut d = Driver::new(&[touch_sub(ETH)]);
    // The docs' shape, prices only: no Touch, counted, no size synthesized.
    assert!(d.feed("abp_price_only").is_empty());
    assert!(d.feed("abp_ask_size_missing").is_empty());
    assert_eq!(d.counts().unsized_touches, 2);
    assert_eq!(d.feed("abp_sized"), [sized()]);
    assert_eq!(d.counts().unsized_touches, 2);
}

#[test]
fn a_later_frame_missing_a_size_never_leaves_the_touch_in_force_until_the_next_sized_frame() {
    // Sized, price-only, sized (Codex r4354155572).
    let mut d = Driver::new(&[touch_sub(ETH)]);
    assert_eq!(d.feed("abp_sized"), [sized()]);
    assert_eq!(d.feed("abp_price_only"), [touch_gap()]);
    // Still no touch: nothing more is pushed until a sized frame restores it.
    assert!(d.feed("abp_price_only").is_empty());
    assert!(d.feed("abp_ask_size_missing").is_empty());
    let restored = touch((2_000_013_000, 2_000_000_000), (2_000_105_000, 500_000_000));
    assert_eq!(d.feed("abp_sized_next"), [restored]);
    assert_eq!(d.counts().unsized_touches, 3);
    // One side's size missing after a Touch gaps it too.
    assert_eq!(d.feed("abp_ask_size_missing"), [touch_gap()]);
}

#[test]
fn a_touch_in_force_when_its_subscription_is_removed_is_gapped_by_the_next_unusable_frame() {
    let mut d = Driver::new(&[touch_sub(ETH)]);
    assert_eq!(d.feed("abp_sized"), [sized()]);
    // Removed and added again on the same connection: nothing is pushed by either, so the Touch
    // the consumer holds is still in force, and the first unusable frame invalidates it.
    d.remove(&[touch_sub(ETH)]);
    assert!(d.feed("abp_price_only").is_empty());
    assert_eq!(d.counts().unsubscribed_frames, 1);
    d.codec
        .subscribe(&[touch_sub(ETH)], &[], &d.specs, &mut d.fx)
        .unwrap();
    assert_eq!(d.feed("abp_price_only"), [touch_gap()]);
    assert!(d.feed("abp_price_only").is_empty());
    assert_eq!(d.feed("abp_sized"), [sized()]);
}

#[test]
fn a_crossed_touch_produces_no_touch_and_gaps_the_touch_feed_until_one_that_is_not() {
    let mut d = Driver::new(&[touch_sub(ETH)]);
    assert_eq!(d.feed("abp_sized"), [sized()]);
    assert_eq!(d.feed("abp_crossed"), [touch_gap()]);
    assert!(d.feed("abp_crossed").is_empty());
    assert_eq!(d.counts().crossed_touches, 2);
    assert_eq!(d.feed("abp_sized"), [sized()]);
    // Bid equal to ask is crossed too ("at or above").
    let locked = frame("abp_sized").replace("2000.012345", "2000.104321");
    let pushed = d.try_text(&locked).unwrap();
    assert_eq!(pushed, [(VenueMeta::NONE, touch_gap())]);
    assert_eq!(d.counts().crossed_touches, 3);
}

#[test]
fn an_unreadable_ask_bid_price_frame_is_counted_and_gaps_a_touch_in_force() {
    let sized_text = frame("abp_sized");
    for (what, from, to) in [
        (
            "zero size",
            r#""askSize":"1.500000000""#,
            r#""askSize":"0""#,
        ),
        (
            "size off step",
            r#""askSize":"1.500000000""#,
            r#""askSize":"1.5000000001""#,
        ),
        (
            "price off tick",
            r#""askPrice":"2000.104321""#,
            r#""askPrice":"2000.1043211""#,
        ),
        ("number", r#""bidSize":"1.250000000""#, r#""bidSize":1.25"#),
        ("data", r#""data":{"#, r#""data":7,"x":{"#),
        (
            "timestamp",
            r#""topic":"ask_bid_price""#,
            r#""topic":"ask_bid_price","timestamp_ms":-1"#,
        ),
    ] {
        let mut d = Driver::new(&[touch_sub(ETH)]);
        d.feed("abp_sized");
        let text = sized_text.replacen(from, to, 1);
        assert_ne!(text, sized_text, "{what}");
        assert_eq!(
            d.try_text(&text).unwrap(),
            [(VenueMeta::NONE, touch_gap())],
            "{what}"
        );
        assert_eq!(d.counts().malformed_touches, 1, "{what}");
    }
    // A null value is a missing one.
    let mut d = Driver::new(&[touch_sub(ETH)]);
    let text = sized_text.replace(r#""bidSize":"1.250000000""#, r#""bidSize":null"#);
    assert!(d.try_text(&text).unwrap().is_empty());
    assert_eq!(d.counts().unsized_touches, 1);
}

fn trade(px: i64, qty: i64, aggressor: Aggressor) -> MdEvent {
    MdEvent::Trade {
        inst: ETH,
        id: None,
        aggressor,
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

fn at_secs(secs: i64) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(secs * 1_000_000_000)),
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: None,
    }
}

#[test]
fn both_trades_shapes_decode_with_the_taker_side_and_a_timestamp_in_seconds() {
    let mut d = Driver::new(&[sub(ETH, Feed::Trades)]);
    let single = d.try_text(&frame("trade_single")).unwrap();
    assert_eq!(
        single,
        [(
            at_secs(1_767_225_600),
            trade(2_000_051_234, 250_000_000, Aggressor::Buyer)
        )]
    );
    let many = d.try_text(&frame("trades_array")).unwrap();
    assert_eq!(
        many,
        [
            (
                at_secs(1_767_225_601),
                trade(2_000_049_876, 100_000_000, Aggressor::Seller)
            ),
            (
                at_secs(1_767_225_601),
                trade(2_000_050_000, 300_000_000, Aggressor::Buyer)
            ),
        ]
    );
    // A trade with no timestamp carries none.
    let text = frame("trade_single").replace(r#","timestamp":1767225600"#, "");
    let pushed = d.try_text(&text).unwrap();
    assert_eq!(pushed[0].0, VenueMeta::NONE);
    // `side`, the alias the Java library reads, where `takerSide` is absent: the same two values.
    let text = frame("trades_array").replace("takerSide", "side");
    assert_eq!(d.try_text(&text).unwrap(), many);
    // `takerSide` is read where both are present.
    let text =
        frame("trade_single").replace(r#""takerSide":"Buy""#, r#""side":"Sell","takerSide":"Buy""#);
    assert_eq!(d.try_text(&text).unwrap(), single);
}

#[test]
fn a_malformed_trades_frame_is_refused_whole() {
    let mut d = Driver::new(&[sub(ETH, Feed::Trades)]);
    let array = frame("trades_array");
    for (what, from, to) in [
        ("side", r#""takerSide":"Buy""#, r#""takerSide":"BUY""#),
        ("no side", r#","takerSide":"Buy""#, ""),
        // The alias holds only the two values: never a buy by default, as the Java library has.
        ("alias side", r#""takerSide":"Buy""#, r#""side":"BUY""#),
        ("alias bid", r#""takerSide":"Buy""#, r#""side":"BID""#),
        (
            "price",
            r#""price":"2000.050000""#,
            r#""price":"2000.0500001""#,
        ),
        ("zero", r#""quantity":"0.300000000""#, r#""quantity":"0""#),
        (
            "timestamp",
            r#""timestamp":1767225601}]"#,
            r#""timestamp":"1767225601"}]"#,
        ),
        (
            "item",
            r#"[{"price":"2000.049876""#,
            r#"[7,{"price":"2000.049876""#,
        ),
        ("shape", r#""trades":["#, r#""trade":7,"x":["#),
    ] {
        let text = array.replacen(from, to, 1);
        assert_ne!(text, array, "{what}");
        assert!(
            matches!(d.try_text(&text), Err(DecodeError::Malformed(_))),
            "{what}"
        );
    }
    let both = frame("trade_single").replace(r#""trade":"#, r#""trades":[],"trade":"#);
    assert!(d.try_text(&both).is_err());
}

#[test]
fn mark_price_and_spot_price_decode_exactly() {
    let mut d = Driver::new(&[sub(ETH, Feed::Mark), sub(ETH, Feed::Index)]);
    let px = |s: &str| s.parse::<PxExact>().unwrap();
    assert_eq!(
        d.feed("mark_price"),
        [MdEvent::Mark {
            inst: ETH,
            px: px("2000.061234")
        }]
    );
    assert_eq!(
        d.feed("spot_price"),
        [MdEvent::Index {
            inst: ETH,
            px: px("2000.071234")
        }]
    );
    // `price`, the alias the Java library reads, where `markPrice` is absent; `markPrice` where
    // both are present. `spot_price` has no alias.
    let mark = |text: &str| MdEvent::Mark {
        inst: ETH,
        px: px(text),
    };
    let text = frame("mark_price").replace("markPrice", "price");
    let pushed = d.try_text(&text).unwrap();
    assert_eq!(pushed, [(VenueMeta::NONE, mark("2000.061234"))]);
    let text = frame("mark_price").replace(r#""markPrice""#, r#""price":"1.5","markPrice""#);
    let pushed = d.try_text(&text).unwrap();
    assert_eq!(pushed, [(VenueMeta::NONE, mark("2000.061234"))]);
    let text = frame("mark_price").replace("markPrice", "mark");
    assert_eq!(d.try_text(&text), Err(DecodeError::Malformed("markPrice")));
    let text = frame("spot_price").replace("spotPrice", "price");
    assert_eq!(d.try_text(&text), Err(DecodeError::Malformed("spotPrice")));
    for name in ["mark_price", "spot_price"] {
        let text = frame(name).replace(r#""data":{"#, r#""data":[],"x":{"#);
        assert_eq!(d.try_text(&text), Err(DecodeError::Malformed("data")));
    }
}

fn funding(rate_e12: i64) -> MdEvent {
    MdEvent::Funding {
        inst: ETH,
        rate_e12,
        interval: None,
        next: None,
    }
}

#[test]
fn funding_decodes_in_both_shapes_exactly_with_no_interval_and_no_next_time() {
    let mut d = Driver::new(&[sub(ETH, Feed::Funding)]);
    // 0.000012 and -0.000007 in units of 1e-12, never rescaled by an interval.
    assert_eq!(d.feed("funding_nested"), [funding(12_000_000)]);
    assert_eq!(d.feed("funding_flat"), [funding(-7_000_000)]);
    assert_eq!(d.feed("funding_finest"), [funding(1)]);
    // A finer rate is refused as malformed.
    let bad = Err(DecodeError::Malformed("estimatedFundingRate"));
    assert_eq!(d.try_text(&frame("funding_too_fine")), bad);
    for (from, to) in [
        (
            r#""estimatedFundingRate":"-0.000007""#,
            r#""estimatedFundingRate":-0.000007"#,
        ),
        (
            r#""estimatedFundingRate":"-0.000007""#,
            r#""estimatedFundingRate":"--0.1""#,
        ),
        (
            r#""estimatedFundingRate":"-0.000007""#,
            r#""estimatedFundingRate":"1","fundingRateEstimation":{}"#,
        ),
        (
            r#""estimatedFundingRate":"-0.000007""#,
            r#""estimatedFundingRate":"99999999999""#,
        ),
    ] {
        let text = frame("funding_flat").replace(from, to);
        assert_eq!(d.try_text(&text), bad, "{to}");
    }
    let text = frame("funding_nested").replace(r#""estimatedFundingRate":"0.000012","#, "");
    assert_eq!(d.try_text(&text), bad);
}

#[test]
fn no_exchange_timestamp_is_synthesized_where_a_frame_has_none() {
    let mut d = Driver::new(&every_feed(ETH));
    d.take_fx();
    for name in [
        "eth_s0",
        "eth_u1",
        "abp_sized",
        "mark_price",
        "spot_price",
        "funding_nested",
        "funding_flat",
    ] {
        let pushed = d.try_text(&frame(name)).unwrap();
        assert!(!pushed.is_empty(), "{name}");
        for (meta, _) in pushed {
            assert_eq!(meta, VenueMeta::NONE, "{name}");
        }
    }
    // A root timestamp_ms where a frame carries one, in milliseconds.
    let text = frame("mark_price").replace(
        r#""topic":"mark_price""#,
        r#""timestamp_ms":1767225600123,"topic":"mark_price""#,
    );
    let pushed = d.try_text(&text).unwrap();
    assert_eq!(pushed[0].0.exch_ts, Some(ExchNs(1_767_225_600_123_000_000)));
    let text = frame("mark_price").replace(
        r#""topic":"mark_price""#,
        r#""timestamp_ms":"x","topic":"mark_price""#,
    );
    assert_eq!(
        d.try_text(&text),
        Err(DecodeError::Malformed("timestamp_ms"))
    );
}

#[test]
fn an_acknowledgement_is_taken_and_frames_outside_the_protocol_are_refused() {
    let mut d = Driver::new(&[book_sub(ETH)]);
    assert!(d.feed("ack").is_empty());
    assert_eq!(d.counts().acks, 1);
    for (text, why) in [
        (
            r#"{"id":1,"status":400}"#,
            "a frame with no topic that is not an acknowledgement",
        ),
        (
            r#"{"subscriptions":[]}"#,
            "a frame with no topic that is not an acknowledgement",
        ),
        ("[]", "not a JSON object"),
        ("{", "not JSON"),
        (r#"{"topic":7}"#, "topic"),
        (
            r#"{"topic":"klines","symbol":"ETH/USDT-P","data":{}}"#,
            "a topic never subscribed",
        ),
        (r#"{"topic":"orderbook","data":{}}"#, "symbol"),
    ] {
        assert_eq!(d.try_text(text), Err(DecodeError::Malformed(why)), "{text}");
    }
    let unknown = r#"{"topic":"orderbook","symbol":"BTC/USDT-P","data":{}}"#;
    assert_eq!(d.try_text(unknown), Err(DecodeError::UnknownInstrument));
    // A topic of a subscribed instrument not subscribed on this connection is passed over.
    assert!(d.feed("mark_price").is_empty());
    assert_eq!(d.counts().unsubscribed_frames, 1);
    // A binary frame is refused.
    let caps = Hibachi.caps(&config()).unwrap();
    let mut sink = Kept::default();
    let result = dispatch_market_data(&caps, |scope| {
        d.codec.on_frame(
            RawFrame::Binary(b"{}"),
            scope,
            &d.specs,
            &mut sink,
            &mut d.fx,
        )
    });
    assert_eq!(result, Err(DecodeError::Malformed("a binary frame")));
}

#[test]
fn the_codec_makes_no_http_request_needs_no_keepalive_and_holds_no_credential() {
    let mut d = Driver::new(&[book_sub(ETH)]);
    d.take_fx();
    let caps = Hibachi.caps(&config()).unwrap();
    let mut sink = Kept::default();
    let answered = dispatch_market_data(&caps, |scope| {
        d.codec.on_http(
            HttpTag(1),
            Err(HttpFailure::NotSent),
            scope,
            &d.specs,
            &mut sink,
            &mut d.fx,
        )
    });
    assert_eq!(
        answered,
        Err(DecodeError::Malformed(
            "no Hibachi market-data request is made"
        ))
    );
    assert_eq!(d.codec.keepalive(), None);
    let text = frame("eth_s0");
    assert_eq!(
        d.codec
            .redact_inbound(Inbound::Frame(RawFrame::Text(&text))),
        InboundSpans::NONE
    );
    // on_open asks for nothing; a timer it never set does nothing.
    let mut fx = Effects::new();
    d.codec.on_open(&mut fx);
    assert!(fx.is_empty());
    assert!(d.fire(TimerTag(99)).is_empty());
    assert!(d.take_fx().is_empty());
    assert!(sink.0.is_empty());
}
