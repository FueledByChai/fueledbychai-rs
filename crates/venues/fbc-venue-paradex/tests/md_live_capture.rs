//! FBC-yj56: Paradex's production order book, as captured on 2026-10-08 from its public SBE 1:1
//! WebSocket (`fixtures/paradex/md/README.md`), decodes through the codec into a book that is
//! gap-free and uncrossed after every frame, on both book channels; and the channel the codec
//! subscribes is, character for character, the one the venue acknowledged (decision 0074).

mod md;

use fbc_book::Books;
use fbc_core::{
    BookId, BookSide, Effect, Effects, MdCodec, MdEvent, RawFrame, StreamId, Subscription,
    dispatch_market_data,
};
use fbc_venue_paradex::factory::caps;
use fbc_venue_paradex::md::book::frame_seq;
use fbc_venue_paradex::md::{DELTAS, INTERACTIVE_DELTAS, ParadexMd};
use md::{BTC, Captured, Collect, capture, live_specs};
use serde_json::Value;

const STREAM: StreamId = StreamId(0);

/// A codec subscribed to BTC's `book`, and the channel its subscribe frame named.
fn subscribed(book: BookId) -> (ParadexMd, String) {
    let mut codec = ParadexMd::new(STREAM);
    let mut fx = Effects::new();
    let sub = Subscription {
        inst: BTC,
        feed: fbc_core::Feed::Book(book),
    };
    codec
        .subscribe(&[sub], &[], &live_specs(), &mut fx)
        .unwrap();
    let [Effect::Send { frame, .. }] = fx.as_slice() else {
        panic!("one subscribe frame: {fx:?}");
    };
    let request: Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(request["method"], "subscribe");
    let channel = request["params"]["channel"].as_str().unwrap().to_owned();
    (codec, channel)
}

/// What a capture replayed to.
struct Replayed {
    frames: usize,
    first_seq: u64,
    last_seq: u64,
    /// The levels per side, bids then asks, of the snapshot that opened the capture.
    snapshot_levels: (usize, usize),
}

/// Replays capture `name` through a codec subscribed to BTC's `book`, building the book from
/// what it pushes, and asserts, frame by frame: the first frame is the channel's
/// acknowledgement of the very channel the codec subscribed; every book frame decodes, pushes
/// no health event and asks for nothing (no gap, no reconnect); its seq_no is the last plus
/// one; the first is a snapshot; and after it the book has both sides and is not crossed.
fn replay(name: &str, book: BookId) -> Replayed {
    let (mut codec, channel) = subscribed(book);
    let specs = live_specs();
    let mut frames = capture(name).into_iter();
    let Some(Captured::Text(ack)) = frames.next() else {
        panic!("{name}: the capture opens with the subscribe acknowledgement");
    };
    let reply: Value = serde_json::from_str(&ack).unwrap();
    assert_eq!(
        reply["result"]["channel"],
        channel.as_str(),
        "{name}: {ack}"
    );
    let mut books = Books::new();
    let mut out: Option<Replayed> = None;
    let decode = |codec: &mut ParadexMd, frame: RawFrame<'_>| {
        let (mut sink, mut fx) = (Collect::default(), Effects::new());
        let result = dispatch_market_data(&caps(), |scope| {
            codec.on_frame(frame, scope, &specs, &mut sink, &mut fx)
        });
        assert_eq!(result, Ok(()), "{name}");
        assert!(fx.is_empty(), "{name}: asked for {fx:?}");
        sink.0
    };
    assert!(decode(&mut codec, RawFrame::Text(&ack)).is_empty());
    for (i, frame) in frames.enumerate() {
        let Captured::Binary(bytes) = frame else {
            panic!("{name}: a text frame after the acknowledgement");
        };
        let events = decode(&mut codec, RawFrame::Binary(&bytes));
        let (inst, seq) = frame_seq(&bytes, &specs).unwrap().expect("a book frame");
        assert_eq!(inst, BTC);
        for (_, ev) in &events {
            assert!(
                !matches!(ev, MdEvent::Health { .. }),
                "{name} frame {i}: {ev:?}"
            );
            books.apply(ev).unwrap();
        }
        match out.as_mut() {
            None => {
                assert!(
                    matches!(events.first(), Some((_, MdEvent::BookSnapshotBegin { .. }))),
                    "{name}: the channel opens with a snapshot"
                );
                let side = |want| {
                    let is =
                        |ev: &MdEvent| matches!(ev, MdEvent::Level { side, .. } if *side == want);
                    events.iter().filter(|(_, ev)| is(ev)).count()
                };
                out = Some(Replayed {
                    frames: 1,
                    first_seq: seq,
                    last_seq: seq,
                    snapshot_levels: (side(BookSide::Bid), side(BookSide::Ask)),
                });
            }
            Some(r) => {
                assert_eq!(seq, r.last_seq + 1, "{name} frame {i}: a seq_no gap");
                r.frames += 1;
                r.last_seq = seq;
            }
        }
        let touch = books.get(BTC, book).unwrap().touch().unwrap();
        let (Some(bid), Some(ask)) = (touch.bid, touch.ask) else {
            panic!("{name} frame {i}: a side is empty: {touch:?}");
        };
        assert!(
            bid.px < ask.px,
            "{name} frame {i} (seq {seq}): crossed {touch:?}"
        );
    }
    out.expect("a book frame")
}

#[test]
fn the_captured_deltas_book_is_gap_free_and_never_crossed() {
    let r = replay("btc-order-book-deltas-2026-10-08.jsonl", DELTAS);
    assert_eq!(r.frames, 289);
    assert_eq!((r.first_seq, r.last_seq), (7_687_289_234, 7_687_289_522));
    // The whole book, not the top 15 the documented `@15` channel names.
    assert_eq!(r.snapshot_levels, (115, 60));
}

#[test]
fn the_captured_interactive_deltas_book_is_gap_free_and_never_crossed() {
    let r = replay(
        "btc-order-book-interactive-deltas-2026-10-08.jsonl",
        INTERACTIVE_DELTAS,
    );
    assert_eq!(r.frames, 299);
    assert_eq!((r.first_seq, r.last_seq), (7_687_289_525, 7_687_289_823));
    assert_eq!(r.snapshot_levels, (120, 64));
}

#[test]
fn the_codec_subscribes_the_bare_channel_names_the_venue_acknowledged() {
    // The venue's own words: each capture's first frame acknowledges its channel; `replay`
    // checks the codec's subscribe against it. Here, the spelling itself.
    for (book, want) in [
        (DELTAS, "order_book.BTC-USD-PERP.deltas"),
        (
            INTERACTIVE_DELTAS,
            "order_book.BTC-USD-PERP.interactive_deltas",
        ),
    ] {
        assert_eq!(subscribed(book).1, want);
    }
}
