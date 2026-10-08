//! FBC-taxd: Paradex's RPI-inclusive touch, `bbo.{market}.interactive`, is a touch source of its
//! own beside the plain `bbo.{market}` (decision 0076). The codec subscribes it under exactly
//! that spelling, the one the venue acknowledged in the 2026-10-08 capture; the captured frames
//! decode into touches the events and the caps name as the interactive source; the plain bbo is
//! subscribed, decoded and declared as before; and since a bbo frame names its market but not
//! its channel, a market's two touch sources never share a connection.

mod md;

use std::collections::BTreeSet;

use fbc_core::{
    Channel, Effect, Effects, ExchTsKind, Feed, MdCodec, MdEvent, MdTransport, RawFrame, SeqDomain,
    StreamId, Subscription, TagSet, TouchSourceId, VenueConfig, VenueError, VenueFactory,
    dispatch_market_data,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::{MD_URL, caps};
use fbc_venue_paradex::md::{BBO, BBO_INTERACTIVE, DELTAS, INTERACTIVE_DELTAS, ParadexMd};
use md::{BTC, Captured, Collect, ETH, capture, decode_with, frame, live_specs, specs};
use serde_json::Value;

const STREAM: StreamId = StreamId(0);

fn sub(inst: fbc_core::InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

/// The channel of each subscribe frame in `fx`, in order.
fn channels(fx: &Effects) -> Vec<String> {
    fx.as_slice()
        .iter()
        .map(|effect| {
            let Effect::Send { frame, .. } = effect else {
                panic!("not a frame: {effect:?}");
            };
            let request: Value = serde_json::from_slice(frame.bytes()).unwrap();
            assert_eq!(request["method"], "subscribe");
            request["params"]["channel"].as_str().unwrap().to_owned()
        })
        .collect()
}

/// A codec subscribed to BTC's touch `source`, and the channel its subscribe frame named.
fn subscribed(source: TouchSourceId) -> (ParadexMd, String) {
    let mut codec = ParadexMd::new(STREAM);
    let mut fx = Effects::new();
    let touch = sub(BTC, Feed::Touch(source));
    codec
        .subscribe(&[touch], &[], &live_specs(), &mut fx)
        .unwrap();
    let [channel] = channels(&fx).try_into().unwrap();
    (codec, channel)
}

/// What a bbo capture replayed to.
struct Replayed {
    touches: usize,
    first_seq: u64,
    last_seq: u64,
    /// The median spread, in hundredths of a basis point, rounded down.
    median_spread_cbps: i64,
}

/// Replays bbo capture `name` through a codec subscribed to BTC's touch `source`, and asserts:
/// the capture opens with the venue's acknowledgement of the very channel the codec
/// subscribed; every frame after it decodes to one `Touch` of BTC from `source`, with both
/// sides, uncrossed, stamped with the frame's publish time and an increasing seq; nothing is
/// asked for.
fn replay(name: &str, source: TouchSourceId) -> Replayed {
    let (mut codec, channel) = subscribed(source);
    let specs = live_specs();
    let mut frames = capture(name).into_iter();
    let Some(Captured::Text(ack)) = frames.next() else {
        panic!("{name}: the capture opens with the subscribe acknowledgement");
    };
    let reply: Value = serde_json::from_str(&ack).unwrap();
    assert_eq!(reply["result"]["channel"], channel.as_str(), "{name}");
    let mut decode = |frame: RawFrame<'_>| {
        let (mut sink, mut fx) = (Collect::default(), Effects::new());
        let result = dispatch_market_data(&caps(), |scope| {
            codec.on_frame(frame, scope, &specs, &mut sink, &mut fx)
        });
        assert_eq!(result, Ok(()), "{name}");
        assert!(fx.is_empty(), "{name}: asked for {fx:?}");
        sink.0
    };
    assert!(decode(RawFrame::Text(&ack)).is_empty());
    let mut seqs = Vec::new();
    let mut spreads = Vec::new();
    for (i, frame) in frames.enumerate() {
        let Captured::Binary(bytes) = frame else {
            panic!("{name}: a text frame after the acknowledgement");
        };
        let events = decode(RawFrame::Binary(&bytes));
        let [
            (
                meta,
                MdEvent::Touch {
                    inst,
                    bid,
                    ask,
                    source: from,
                },
            ),
        ] = events.as_slice()
        else {
            panic!("{name} frame {i}: not one touch: {events:?}");
        };
        assert_eq!((*inst, *from), (BTC, source), "{name} frame {i}");
        assert_eq!(meta.exch_ts_kind, ExchTsKind::Publish);
        assert!(meta.exch_ts.is_some());
        let (Some(bid), Some(ask)) = (bid, ask) else {
            panic!("{name} frame {i}: a side is empty");
        };
        assert!(bid.px < ask.px, "{name} frame {i}: crossed {bid:?} {ask:?}");
        let seq = meta.venue_seq.expect("a seq");
        if let Some(&last) = seqs.last() {
            assert!(seq > last, "{name} frame {i}: seq {seq} after {last}");
        }
        seqs.push(seq);
        // Ticks of 0.1: spread / mid in hundredths of a basis point.
        let (b, a) = (bid.px.0, ask.px.0);
        spreads.push((a - b) * 2 * 1_000_000 / (a + b));
    }
    spreads.sort_unstable();
    Replayed {
        touches: seqs.len(),
        first_seq: seqs[0],
        last_seq: *seqs.last().unwrap(),
        median_spread_cbps: spreads[spreads.len() / 2],
    }
}

#[test]
fn the_interactive_touch_is_subscribed_as_exactly_bbo_market_interactive() {
    // The venue acknowledges this spelling and streams it; `bbo.interactive.{market}` and
    // `bbo.{market}@interactive` are acknowledged too but stream nothing, and
    // `bbo_interactive.{market}`, `interactive_bbo.{market}` and `rpi_bbo.{market}` are refused
    // (the 2026-10-07 probe, decision 0076). Only the exact name is right.
    assert_eq!(
        subscribed(BBO_INTERACTIVE).1,
        "bbo.BTC-USD-PERP.interactive"
    );
    // The plain touch keeps its channel.
    assert_eq!(subscribed(BBO).1, "bbo.BTC-USD-PERP");
    // Another market's interactive touch is spelled the same way.
    let mut codec = ParadexMd::new(STREAM);
    let mut fx = Effects::new();
    let eth = sub(ETH, Feed::Touch(BBO_INTERACTIVE));
    codec.subscribe(&[eth], &[], &specs(), &mut fx).unwrap();
    assert_eq!(channels(&fx), ["bbo.ETH-USD-PERP.interactive"]);
}

#[test]
fn the_captured_interactive_touches_decode_as_the_interactive_source() {
    let r = replay("btc-bbo-interactive-2026-10-08.jsonl", BBO_INTERACTIVE);
    assert_eq!(r.touches, 299);
    assert_eq!((r.first_seq, r.last_seq), (7_687_292_177, 7_687_292_528));
    // About 2.35 bps, the interactive touch's median spread in its 25 s.
    assert_eq!(r.median_spread_cbps, 234);
}

#[test]
fn the_captured_plain_touches_decode_as_the_plain_bbo_as_before() {
    let r = replay("btc-bbo-2026-10-08.jsonl", BBO);
    assert_eq!(r.touches, 299);
    assert_eq!((r.first_seq, r.last_seq), (7_687_292_570, 7_687_292_898));
    // About 3.62 bps, the plain touch's median spread in its 12 s (captured just after the
    // other, not beside it).
    assert_eq!(r.median_spread_cbps, 361);
}

#[test]
fn the_caps_declare_the_interactive_touch_beside_the_unchanged_bbo() {
    let md = caps().md;
    assert_eq!(md.touch_sources.len(), 2);
    let plain = md.touch_sources[usize::from(BBO.0)];
    assert_eq!(plain.channel, "bbo");
    assert_eq!(plain.includes_channels, TagSet::of(&[Channel::Public]));
    assert_eq!(plain.seq_domain, SeqDomain::SharedWithBook);
    let interactive = md.touch_sources[usize::from(BBO_INTERACTIVE.0)];
    assert_eq!(interactive.channel, "bbo.interactive");
    assert_eq!(
        interactive.includes_channels,
        TagSet::of(&[Channel::Public, Channel::Rpi])
    );
    // The same BboEvent, the same timestamp and the same orderbook counter as the plain bbo.
    assert_eq!(
        (
            interactive.cadence,
            interactive.seq_domain,
            interactive.ts_kind
        ),
        (plain.cadence, plain.seq_domain, plain.ts_kind)
    );
    // Each touch shows what its order book shows: the interactive touch is the interactive
    // book's, the plain bbo the deltas book's.
    let book = |id: fbc_core::BookId| md.books[usize::from(id.0)].includes_channels;
    assert_eq!(interactive.includes_channels, book(INTERACTIVE_DELTAS));
    assert_eq!(plain.includes_channels, book(DELTAS));
}

#[test]
fn a_markets_two_touch_sources_never_share_a_connection() {
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, "wss://ws.api.prod.paradex.trade/v1");
    let subs: BTreeSet<_> = [
        sub(BTC, Feed::Touch(BBO)),
        sub(BTC, Feed::Touch(BBO_INTERACTIVE)),
        sub(BTC, Feed::Trades),
        sub(BTC, Feed::Book(DELTAS)),
        sub(ETH, Feed::Touch(BBO_INTERACTIVE)),
    ]
    .into();
    let plans = ParadexFactory.plan_md(&cfg, &specs(), &subs).unwrap();
    let planned: Vec<_> = plans.iter().map(|p| (p.stream, p.subs.clone())).collect();
    assert_eq!(
        planned,
        [
            // The plain bbo goes on the first connection with the trades and the first book
            // channel, as before; every market's interactive touch on the second.
            (
                StreamId(0),
                vec![
                    sub(BTC, Feed::Touch(BBO)),
                    sub(BTC, Feed::Book(DELTAS)),
                    sub(BTC, Feed::Trades),
                ]
            ),
            (
                StreamId(1),
                vec![
                    sub(BTC, Feed::Touch(BBO_INTERACTIVE)),
                    sub(ETH, Feed::Touch(BBO_INTERACTIVE)),
                ]
            ),
        ]
    );
    for plan in &plans {
        assert!(matches!(plan.transport, MdTransport::Socket { .. }));
    }
    // Both touch sources and both books of one market: two connections, not four.
    let all: BTreeSet<_> = [
        sub(BTC, Feed::Touch(BBO)),
        sub(BTC, Feed::Touch(BBO_INTERACTIVE)),
        sub(BTC, Feed::Book(DELTAS)),
        sub(BTC, Feed::Book(INTERACTIVE_DELTAS)),
    ]
    .into();
    let plans = ParadexFactory.plan_md(&cfg, &specs(), &all).unwrap();
    assert_eq!(plans.len(), 2);

    // The codec refuses a market's second touch source on its connection, sending nothing,
    // even once the first is removed: a frame still in flight names only its market.
    let mut codec = ParadexMd::new(STREAM);
    let mut fx = Effects::new();
    let (plain, interactive) = (
        sub(BTC, Feed::Touch(BBO)),
        sub(BTC, Feed::Touch(BBO_INTERACTIVE)),
    );
    let both = codec.subscribe(&[plain, interactive], &[], &specs(), &mut fx);
    assert_eq!(both, Err(VenueError::UnsupportedFeed(interactive)));
    assert!(fx.is_empty(), "a refusal sends nothing");
    codec
        .subscribe(&[interactive], &[], &specs(), &mut fx)
        .unwrap();
    codec
        .subscribe(&[], &[interactive], &specs(), &mut fx)
        .unwrap();
    let fx_before = fx.len();
    let again = codec.subscribe(&[plain], &[], &specs(), &mut fx);
    assert_eq!(again, Err(VenueError::UnsupportedFeed(plain)));
    assert_eq!(fx.len(), fx_before);
    // The bbo frame in flight after the unsubscribe is the interactive touch's.
    let out = decode_with(&mut codec, RawFrame::Binary(&frame("bbo.sbe.txt")));
    let [(_, MdEvent::Touch { source, .. })] = out.events.as_slice() else {
        panic!("{:?}", out.events);
    };
    assert_eq!(*source, BBO_INTERACTIVE);
}

#[test]
fn a_refused_interactive_touch_is_reported_as_itself_and_its_frames_are_not_pushed() {
    let mut codec = ParadexMd::new(STREAM);
    let mut fx = Effects::new();
    let interactive = sub(BTC, Feed::Touch(BBO_INTERACTIVE));
    codec
        .subscribe(&[interactive], &[], &specs(), &mut fx)
        .unwrap();
    let out = decode_with(&mut codec, RawFrame::Text(&md::rpc_error(1)));
    assert_eq!(out.result, Ok(()));
    assert_eq!(out.events, [md::refused_sub(BTC, interactive.feed)]);
    let out = decode_with(&mut codec, RawFrame::Binary(&frame("bbo.sbe.txt")));
    assert_eq!(out.result, Ok(()));
    assert!(out.events.is_empty(), "{:?}", out.events);
}

/// Codex r4214305885: a touch source keeps its connection whatever else the desired set holds,
/// so a change of the set never asks a running connection's codec to swap one source of a
/// market for the other, which it refuses. The interactive touch is always on the second
/// connection, the plain bbo always on the first.
#[test]
fn a_touch_source_keeps_its_connection_as_the_desired_set_changes() {
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, "wss://ws.api.prod.paradex.trade/v1");
    let plan = |subs: &[Subscription]| {
        let subs: BTreeSet<_> = subs.iter().copied().collect();
        let plans = ParadexFactory.plan_md(&cfg, &specs(), &subs).unwrap();
        plans
            .into_iter()
            .map(|p| (p.stream, p.subs))
            .collect::<Vec<_>>()
    };
    let (plain, interactive) = (
        sub(BTC, Feed::Touch(BBO)),
        sub(BTC, Feed::Touch(BBO_INTERACTIVE)),
    );
    let trades = sub(BTC, Feed::Trades);
    // The interactive touch alone, then beside the plain one, then the plain one alone.
    assert_eq!(plan(&[interactive]), [(StreamId(1), vec![interactive])]);
    assert_eq!(
        plan(&[interactive, trades]),
        [
            (StreamId(0), vec![trades]),
            (StreamId(1), vec![interactive])
        ]
    );
    assert_eq!(
        plan(&[plain, interactive, trades]),
        [
            (StreamId(0), vec![plain, trades]),
            (StreamId(1), vec![interactive])
        ]
    );
    assert_eq!(plan(&[plain, trades]), [(StreamId(0), vec![plain, trades])]);
    // So each step's difference on a running connection is one its codec takes: the
    // second connection only ever holds the interactive touch, the first only the plain one.
    let mut first = ParadexMd::new(StreamId(0));
    let mut second = ParadexMd::new(StreamId(1));
    let mut fx = Effects::new();
    second
        .subscribe(&[interactive], &[], &specs(), &mut fx)
        .unwrap();
    first.subscribe(&[trades], &[], &specs(), &mut fx).unwrap();
    first.subscribe(&[plain], &[], &specs(), &mut fx).unwrap();
    second
        .subscribe(&[], &[interactive], &specs(), &mut fx)
        .unwrap();
}
