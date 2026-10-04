//! FBC-70f: Paradex SBE `BookEvent` frames decode into book events on the subscribed book
//! channel; seq_no is tracked per book, and a skipped or backwards seq_no pushes a gap and asks
//! for a reconnect of the book's stream (decision 0022), with no subscribe frame from the codec
//! and no delta applied before the fresh snapshot; plan_md never puts two book channels of one
//! market on one connection.

mod md;

use std::collections::BTreeSet;

use fbc_core::{
    BookId, BookSide, DecodeError, Effect, ExchNs, ExchTsKind, Feed, FeedHealth, InstrumentId,
    Lots, MdCodec, MdEvent, RawFrame, StreamId, Subscription, Ticks, VenueConfig, VenueError,
    VenueFactory, VenueMeta,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::{MD_URL, caps};
use fbc_venue_paradex::md::{BBO, DELTAS, INTERACTIVE_DELTAS, ParadexMd};
use md::{BTC, Decoded, ETH, decode_with, frame, raw, specs};

/// BookEvent.ts in the hand-built fixtures, in microseconds.
const TS_US: i64 = 1_759_500_000_123_456;
const STREAM: StreamId = StreamId(3);

fn sub(inst: InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

/// A codec on `STREAM` subscribed to `book` of BTC.
fn codec(book: BookId) -> ParadexMd {
    let mut codec = ParadexMd::new(STREAM);
    let mut fx = fbc_core::Effects::new();
    let subs = [sub(BTC, Feed::Book(book))];
    codec.subscribe(&subs, &[], &specs(), &mut fx).unwrap();
    codec
}

fn feed(codec: &mut ParadexMd, bytes: &[u8]) -> Decoded {
    decode_with(codec, RawFrame::Binary(bytes))
}

fn meta(seq: u64) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(TS_US * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq),
    }
}

fn level(book: BookId, side: BookSide, px: i64, qty: i64) -> MdEvent {
    MdEvent::Level {
        inst: BTC,
        book,
        side,
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

/// `book-snapshot.sbe.txt` as events on `book` under `epoch`.
fn snapshot_events(book: BookId, epoch: u32) -> Vec<(VenueMeta, MdEvent)> {
    let m = meta(1000);
    vec![
        (
            m,
            MdEvent::BookSnapshotBegin {
                inst: BTC,
                book,
                epoch,
            },
        ),
        // 62000.5 x 0.25 and 62000.4 x 1.0 against 62001.0 x 1.5 and 62001.5 x 2.0.
        (m, level(book, BookSide::Bid, 620_005, 250)),
        (m, level(book, BookSide::Bid, 620_004, 1_000)),
        (m, level(book, BookSide::Ask, 620_010, 1_500)),
        (m, level(book, BookSide::Ask, 620_015, 2_000)),
        (m, MdEvent::BookSnapshotEnd { inst: BTC, book }),
    ]
}

fn gap() -> MdEvent {
    MdEvent::Health {
        inst: BTC,
        feed: Feed::Book(DELTAS),
        h: FeedHealth::Gap,
    }
}

/// Asserts `out` pushed `events` and asked for nothing.
fn pushed(out: &Decoded, events: &[(VenueMeta, MdEvent)]) {
    assert_eq!(out.result, Ok(()));
    assert_eq!(out.events, events);
    assert!(out.fx.is_empty(), "{:?}", out.fx);
}

#[test]
fn a_snapshot_decodes_into_begin_its_levels_and_end_on_the_subscribed_book() {
    for book in [DELTAS, INTERACTIVE_DELTAS] {
        let mut codec = codec(book);
        let out = feed(&mut codec, &frame("book-snapshot.sbe.txt"));
        pushed(&out, &snapshot_events(book, 1));
        // Every snapshot starts the next epoch of that book.
        let out = feed(&mut codec, &frame("book-snapshot.sbe.txt"));
        pushed(&out, &snapshot_events(book, 2));
    }
}

#[test]
fn a_continuous_sequence_applies_every_delta_and_pushes_no_gap() {
    let mut codec = codec(DELTAS);
    feed(&mut codec, &frame("book-snapshot.sbe.txt"));
    // 1001 removes 62000.4 (size 0) and sets 62001.0 to 1.25; 1002 adds 62000.6 x 0.5.
    let out = feed(&mut codec, &frame("book-delta-1001.sbe.txt"));
    pushed(
        &out,
        &[
            (meta(1001), level(DELTAS, BookSide::Bid, 620_004, 0)),
            (meta(1001), level(DELTAS, BookSide::Ask, 620_010, 1_250)),
        ],
    );
    let out = feed(&mut codec, &frame("book-delta-1002.sbe.txt"));
    pushed(
        &out,
        &[(meta(1002), level(DELTAS, BookSide::Bid, 620_006, 500))],
    );
    // 1003 has 24-byte entries: the 8 bytes past price and size are skipped.
    let out = feed(&mut codec, &frame("book-longer-entries.sbe.txt"));
    pushed(
        &out,
        &[
            (meta(1003), level(DELTAS, BookSide::Bid, 620_005, 250)),
            (meta(1003), level(DELTAS, BookSide::Bid, 620_004, 0)),
            (meta(1003), level(DELTAS, BookSide::Ask, 620_010, 1_500)),
        ],
    );
}

/// The codec after the snapshot at 1000 and the deltas at 1001 and 1002.
fn at_1002() -> ParadexMd {
    let mut codec = codec(DELTAS);
    for name in [
        "book-snapshot.sbe.txt",
        "book-delta-1001.sbe.txt",
        "book-delta-1002.sbe.txt",
    ] {
        assert_eq!(feed(&mut codec, &frame(name)).result, Ok(()), "{name}");
    }
    codec
}

/// Asserts `out` reported a gap on BTC's deltas book at `seq` and asked only for a reconnect
/// of the book's stream: no frame, so no subscribe.
fn gapped(out: &Decoded, seq: u64) {
    assert_eq!(out.result, Ok(()));
    assert_eq!(out.events, [(meta(seq), gap())]);
    assert_eq!(
        out.fx.as_slice(),
        [Effect::Reconnect {
            stream: STREAM,
            reason: "Paradex book seq_no discontinuity",
        }]
    );
}

/// After a gap, deltas are dropped, even the one that would have followed, until a snapshot
/// arrives; the snapshot then applies, and the deltas after it.
fn resyncs(mut codec: ParadexMd) {
    for name in [
        "book-delta-1004.sbe.txt",
        "book-delta-1001.sbe.txt",
        "book-longer-entries.sbe.txt",
    ] {
        let out = feed(&mut codec, &frame(name));
        pushed(&out, &[]);
    }
    let out = feed(&mut codec, &frame("book-snapshot.sbe.txt"));
    pushed(&out, &snapshot_events(DELTAS, 2));
    let out = feed(&mut codec, &frame("book-delta-1001.sbe.txt"));
    assert_eq!(out.events.len(), 2);
    assert!(out.fx.is_empty());
}

#[test]
fn a_skipped_seq_no_pushes_a_gap_asks_for_a_reconnect_and_applies_nothing_until_a_snapshot() {
    let mut codec = at_1002();
    // 1003 is skipped: the delta at 1004 is not applied.
    let out = feed(&mut codec, &frame("book-delta-1004.sbe.txt"));
    gapped(&out, 1004);
    resyncs(codec);
}

#[test]
fn a_backwards_seq_no_pushes_a_gap_asks_for_a_reconnect_and_applies_nothing_until_a_snapshot() {
    let mut codec = at_1002();
    // 1001 again, after 1002: not after the last.
    let out = feed(&mut codec, &frame("book-delta-1001.sbe.txt"));
    gapped(&out, 1001);
    resyncs(codec);
    // A repeat of the last seq_no is not after it either.
    let mut codec = at_1002();
    gapped(&feed(&mut codec, &frame("book-delta-1002.sbe.txt")), 1002);
}

#[test]
fn deltas_before_the_first_snapshot_are_dropped_without_a_gap() {
    let mut codec = codec(DELTAS);
    for name in ["book-delta-1001.sbe.txt", "book-delta-1002.sbe.txt"] {
        pushed(&feed(&mut codec, &frame(name)), &[]);
    }
    pushed(
        &feed(&mut codec, &frame("book-snapshot.sbe.txt")),
        &snapshot_events(DELTAS, 1),
    );
}

#[test]
fn the_captured_btc_delta_frame_decodes_into_a_level_removal() {
    // Captured from Paradex's production feed by FueledByChaiTrading's Java test on 2026-09-23
    // (fixtures/paradex/md/README.md): seq_no 7678386728, one ask level at 84209.4 removed.
    let captured = raw("btc-book-delta-2026-09-23.sbe");
    let mut snapshot = frame("book-snapshot.sbe.txt");
    snapshot[8 + 8..8 + 16].copy_from_slice(&7_678_386_727i64.to_le_bytes());
    let mut codec = codec(DELTAS);
    assert_eq!(feed(&mut codec, &snapshot).result, Ok(()));
    let out = feed(&mut codec, &captured);
    let meta = VenueMeta {
        exch_ts: Some(ExchNs(1_790_187_959_762_000 * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(7_678_386_728),
    };
    pushed(&out, &[(meta, level(DELTAS, BookSide::Ask, 842_094, 0))]);
}

#[test]
fn a_book_frame_for_a_market_with_no_book_on_the_connection_is_skipped() {
    // ETH has no book subscription here, and BTC's book was unsubscribed.
    let mut eth = frame("book-snapshot.sbe.txt");
    let market = eth.len() - 12;
    eth[market..].copy_from_slice(b"ETH-USD-PERP");
    let mut codec = codec(DELTAS);
    pushed(&feed(&mut codec, &eth), &[]);
    let mut fx = fbc_core::Effects::new();
    let book = [sub(BTC, Feed::Book(DELTAS))];
    codec.subscribe(&[], &book, &specs(), &mut fx).unwrap();
    pushed(&feed(&mut codec, &frame("book-snapshot.sbe.txt")), &[]);
}

#[test]
fn malformed_book_frames_are_refused_with_nothing_pushed_and_the_sequence_kept() {
    let snapshot = frame("book-snapshot.sbe.txt");
    let bids_at = 8 + 89;
    let mut bad_pkg = snapshot.clone();
    bad_pkg[8 + 16] = 2;
    let mut off_grid = snapshot.clone();
    // The last ask's price, 62001.55: off the 0.1 tick.
    let last_ask = snapshot.len() - 13 - 16;
    off_grid[last_ask..last_ask + 8].copy_from_slice(&6_200_155_000_000i64.to_le_bytes());
    let mut short_entry = snapshot.clone();
    // Bids' two 16-byte entries declared as four of 8 bytes: each size is past its entry.
    short_entry[bids_at..bids_at + 4].copy_from_slice(&[8, 0, 4, 0]);
    let mut negative = snapshot.clone();
    negative[8 + 8..8 + 16].copy_from_slice(&(-1i64).to_le_bytes());
    let mut no_market = snapshot.clone();
    no_market.truncate(snapshot.len() - 13);
    let cases = [
        (bad_pkg, DecodeError::Malformed("book pkgType")),
        (
            off_grid,
            DecodeError::Malformed("price off the instrument's grid"),
        ),
        (short_entry, DecodeError::Malformed("book level")),
        (negative, DecodeError::Malformed("negative seq")),
        (no_market, DecodeError::Malformed("SBE market")),
        (
            snapshot[..bids_at + 20].to_vec(),
            DecodeError::Malformed("SBE group runs past the frame"),
        ),
        (
            snapshot[..8 + 16].to_vec(),
            DecodeError::Malformed("SBE frame shorter than its declared block"),
        ),
    ];
    let mut codec = at_1002();
    for (bytes, err) in cases {
        let out = feed(&mut codec, &bytes);
        assert_eq!(out.result, Err(err));
        assert!(out.events.is_empty() && out.fx.is_empty(), "{err:?}");
    }
    // A pkgType past a shorter block is required too.
    let mut short_block = 16u16.to_le_bytes().to_vec();
    short_block.extend_from_slice(&snapshot[2..8 + 16]);
    short_block.extend_from_slice(&snapshot[8 + 89..]);
    let out = feed(&mut codec, &short_block);
    assert_eq!(out.result, Err(DecodeError::Malformed("book pkgType")));
    // None of them moved the sequence: 1003 still follows.
    let out = feed(&mut codec, &frame("book-longer-entries.sbe.txt"));
    assert_eq!((out.events.len(), out.fx.len()), (3, 0));
}

#[test]
fn the_codec_subscribes_each_book_channel_by_its_documented_name() {
    let mut codec = ParadexMd::new(STREAM);
    let mut fx = fbc_core::Effects::new();
    let subs = [
        sub(BTC, Feed::Book(DELTAS)),
        sub(ETH, Feed::Book(INTERACTIVE_DELTAS)),
    ];
    codec.subscribe(&subs, &[], &specs(), &mut fx).unwrap();
    let channels: Vec<String> = fx
        .as_slice()
        .iter()
        .map(|effect| {
            let Effect::Send { frame, .. } = effect else {
                panic!("not a frame: {effect:?}");
            };
            let text: serde_json::Value = serde_json::from_slice(frame.bytes()).unwrap();
            text["params"]["channel"].as_str().unwrap().to_owned()
        })
        .collect();
    assert_eq!(
        channels,
        [
            "order_book.BTC-USD-PERP.deltas@15@50ms",
            "order_book.ETH-USD-PERP.interactive_deltas@15@50ms"
        ]
    );
}

#[test]
fn the_codec_refuses_a_second_book_channel_of_one_market_and_sends_nothing() {
    let mut codec = codec(DELTAS);
    let mut fx = fbc_core::Effects::new();
    let second = sub(BTC, Feed::Book(INTERACTIVE_DELTAS));
    let refused = codec.subscribe(&[second], &[], &specs(), &mut fx);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(second)));
    // Two at once are refused the same way.
    let mut fresh = ParadexMd::new(STREAM);
    let both = [sub(BTC, Feed::Book(DELTAS)), second];
    let refused = fresh.subscribe(&both, &[], &specs(), &mut fx);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(second)));
    assert!(fx.is_empty(), "a refusal sends nothing");
    // The refusal changed nothing: BTC's deltas book still decodes, and the interactive one
    // is taken once the deltas one is removed in the same call.
    pushed(
        &feed(&mut codec, &frame("book-snapshot.sbe.txt")),
        &snapshot_events(DELTAS, 1),
    );
    let deltas = [sub(BTC, Feed::Book(DELTAS))];
    codec
        .subscribe(&[second], &deltas, &specs(), &mut fx)
        .unwrap();
    assert_eq!(fx.len(), 2);
    pushed(
        &feed(&mut codec, &frame("book-snapshot.sbe.txt")),
        &snapshot_events(INTERACTIVE_DELTAS, 1),
    );
    // A book channel this adapter does not declare is refused.
    let undeclared = sub(BTC, Feed::Book(BookId(2)));
    let refused = codec.subscribe(&[undeclared], &[], &specs(), &mut fx);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(undeclared)));
}

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, "wss://ws.api.prod.paradex.trade/v1");
    cfg
}

#[test]
fn plan_md_never_puts_two_book_channels_of_one_market_on_one_connection() {
    let subs: BTreeSet<_> = [
        sub(BTC, Feed::Touch(BBO)),
        sub(BTC, Feed::Trades),
        sub(BTC, Feed::Book(DELTAS)),
        sub(BTC, Feed::Book(INTERACTIVE_DELTAS)),
        sub(ETH, Feed::Book(INTERACTIVE_DELTAS)),
    ]
    .into();
    let plans = ParadexFactory.plan_md(&cfg(), &specs(), &subs).unwrap();
    let planned: Vec<_> = plans.iter().map(|p| (p.stream, p.subs.clone())).collect();
    assert_eq!(
        planned,
        [
            (
                StreamId(0),
                vec![
                    sub(BTC, Feed::Touch(BBO)),
                    sub(BTC, Feed::Book(DELTAS)),
                    sub(BTC, Feed::Trades),
                    sub(ETH, Feed::Book(INTERACTIVE_DELTAS)),
                ]
            ),
            (StreamId(1), vec![sub(BTC, Feed::Book(INTERACTIVE_DELTAS))]),
        ]
    );
    for plan in &plans {
        let mut markets = BTreeSet::new();
        for s in &plan.subs {
            if let Feed::Book(_) = s.feed {
                assert!(markets.insert(s.inst), "two books of one market: {plan:?}");
            }
        }
        // Every connection negotiates SBE, and its codec takes its subscriptions.
        assert_eq!(plan.transport, plans[0].transport);
        let mut codec = ParadexFactory.md_codec(&cfg(), plan);
        let mut fx = fbc_core::Effects::new();
        codec.subscribe(&plan.subs, &[], &specs(), &mut fx).unwrap();
        assert_eq!(fx.len(), plan.subs.len());
    }
    // One book channel per market fits one connection.
    let one: BTreeSet<_> = [sub(BTC, Feed::Book(DELTAS)), sub(ETH, Feed::Book(DELTAS))].into();
    assert_eq!(
        ParadexFactory
            .plan_md(&cfg(), &specs(), &one)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn the_factory_declares_both_book_channels_with_their_continuity() {
    use fbc_core::{Cadence, Channel, Continuity, QueueModelQuality, SeqDomain, TagSet};
    let md = caps().md;
    let channels: Vec<_> = md.books.iter().map(|b| b.channel).collect();
    assert_eq!(channels, ["deltas@15@50ms", "interactive_deltas@15@50ms"]);
    for book in &md.books {
        assert_eq!(book.max_depth, 15);
        assert_eq!(
            book.cadence,
            Cadence::Capped(std::time::Duration::from_millis(50))
        );
        assert_eq!(book.continuity, Continuity::PlusOne);
        assert!(!book.windowed && !book.rest_anchor);
        assert_eq!(book.queue_model, QueueModelQuality::BracketOnly);
    }
    assert_eq!(
        md.books[0].includes_channels,
        TagSet::of(&[Channel::Public])
    );
    assert_eq!(
        md.books[1].includes_channels,
        TagSet::of(&[Channel::Public, Channel::Rpi])
    );
    assert_eq!(md.touch_sources[0].seq_domain, SeqDomain::SharedWithBook);
}
