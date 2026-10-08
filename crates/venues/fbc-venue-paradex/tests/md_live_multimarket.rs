//! FBC-2976: book channels of different Paradex markets share one SBE session. One session of
//! the public production WebSocket, captured on 2026-10-08 (`fixtures/paradex/md/README.md`),
//! subscribed `order_book.BTC-USD-PERP.deltas`, `order_book.ETH-USD-PERP.deltas` and
//! `order_book.SOL-USD-PERP.interactive_deltas` in turn: the venue acknowledged all three and
//! streamed all three markets' books on it. `plan_md` puts those three subscriptions on one
//! connection, the codec built for it sends the probe's three subscribe frames, every
//! acknowledgement answers one of them by the channel it named, and each market's frames build
//! its own book only, gap-free and uncrossed after every frame (decisions 0022 item 4, 0074 and
//! 0077).

mod md;

use std::collections::{BTreeMap, BTreeSet};

use fbc_book::Books;
use fbc_core::{
    BookId, BookSide, Effect, Effects, EndpointPlan, Feed, InstrumentId, MdCodec, MdEvent,
    PriceGrid, RawFrame, SizeStep, SpecTable, Subscription, VenueConfig, VenueFactory,
    dispatch_market_data,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::{MD_URL, caps};
use fbc_venue_paradex::md::book::frame_seq;
use fbc_venue_paradex::md::{DELTAS, INTERACTIVE_DELTAS, ParadexMd};
use md::{BTC, Captured, Collect, ETH, capture, spec};
use rust_decimal::Decimal;
use serde_json::Value;

const SOL: InstrumentId = InstrumentId::new(3);

/// The capture: one session, three book channels of three markets.
const CAPTURE: &str = "multimarket-order-book-2026-10-08.jsonl";

/// The probe's subscriptions, in the order it sent them (JSON-RPC ids 1, 2 and 3).
const PROBED: [Subscription; 3] = [
    Subscription {
        inst: BTC,
        feed: Feed::Book(DELTAS),
    },
    Subscription {
        inst: ETH,
        feed: Feed::Book(DELTAS),
    },
    Subscription {
        inst: SOL,
        feed: Feed::Book(INTERACTIVE_DELTAS),
    },
];

/// The book channel each market holds on the session.
fn book_of(inst: InstrumentId) -> BookId {
    PROBED
        .iter()
        .find_map(|s| match s.feed {
            Feed::Book(book) if s.inst == inst => Some(book),
            _ => None,
        })
        .unwrap()
}

/// The three markets on Paradex's grids: the price tick and size step every level of the
/// capture lies on (the greatest common divisor of its prices and of its sizes, per market).
fn venue_specs() -> SpecTable {
    let mut table = SpecTable::new();
    for (id, symbol, tick, step) in [
        (BTC, "BTC-USD-PERP", Decimal::new(1, 1), Decimal::new(1, 5)),
        (ETH, "ETH-USD-PERP", Decimal::new(1, 2), Decimal::new(1, 4)),
        (SOL, "SOL-USD-PERP", Decimal::new(1, 3), Decimal::new(1, 2)),
    ] {
        let mut market = spec(id, symbol);
        market.price_grid = PriceGrid::fixed(tick).unwrap();
        market.size_step = SizeStep::new(step).unwrap();
        table.insert(market);
    }
    table
}

/// The one endpoint `plan_md` plans for the probe's subscriptions.
fn plan() -> EndpointPlan {
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, "wss://ws.api.prod.paradex.trade/v1");
    let subs = BTreeSet::from(PROBED);
    let mut plans = ParadexFactory.plan_md(&cfg, &venue_specs(), &subs).unwrap();
    assert_eq!(plans.len(), 1, "{plans:?}");
    plans.remove(0)
}

/// The codec built for [`plan`] after subscribing its subscriptions, and each subscribe frame
/// it sent: its JSON-RPC id and channel.
fn subscribed() -> (ParadexMd, Vec<(u64, String)>) {
    let ep = plan();
    let mut codec = ParadexMd::new(ep.stream);
    let mut fx = Effects::new();
    codec
        .subscribe(&ep.subs, &[], &venue_specs(), &mut fx)
        .unwrap();
    let sent = fx
        .as_slice()
        .iter()
        .map(|effect| {
            let Effect::Send { frame, .. } = effect else {
                panic!("only subscribe frames: {effect:?}");
            };
            let request: Value = serde_json::from_slice(frame.bytes()).unwrap();
            assert_eq!(request["method"], "subscribe");
            let id = request["id"].as_u64().unwrap();
            (
                id,
                request["params"]["channel"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    (codec, sent)
}

#[test]
fn plan_md_puts_the_probed_markets_books_on_the_one_connection_the_venue_accepted() {
    let ep = plan();
    assert_eq!(ep.subs, PROBED);
    let fbc_core::MdTransport::Socket { url } = &ep.transport else {
        panic!("a socket: {:?}", ep.transport);
    };
    // The URL the probe opened.
    assert_eq!(
        url.as_str(),
        "wss://ws.api.prod.paradex.trade/v1?sbeSchemaId=1&sbeSchemaVersion=1"
    );
    let (_, sent) = subscribed();
    assert_eq!(
        sent,
        [
            (1, "order_book.BTC-USD-PERP.deltas".to_owned()),
            (2, "order_book.ETH-USD-PERP.deltas".to_owned()),
            (3, "order_book.SOL-USD-PERP.interactive_deltas".to_owned()),
        ]
    );
}

#[test]
fn the_venue_acknowledged_every_market_s_book_channel_on_the_one_session() {
    let (_, sent) = subscribed();
    let acked: BTreeMap<u64, String> = capture(CAPTURE)
        .into_iter()
        .filter_map(|frame| match frame {
            Captured::Text(text) => Some(text),
            Captured::Binary(_) => None,
        })
        .map(|text| {
            let reply: Value = serde_json::from_str(&text).unwrap();
            assert!(reply.get("error").is_none(), "refused: {text}");
            let id = reply["id"].as_u64().unwrap();
            let channel = reply["result"]["channel"].as_str().unwrap().to_owned();
            (id, channel)
        })
        .collect();
    // One acknowledgement per subscribe frame, each naming the channel that frame named: no
    // refusal, no other reply.
    assert_eq!(acked, sent.into_iter().collect::<BTreeMap<_, _>>());
}

/// What one market's frames replayed to.
#[derive(Debug, Eq, PartialEq)]
struct Market {
    frames: usize,
    first_seq: u64,
    last_seq: u64,
    /// The levels per side, bids then asks, of the snapshot that opened its channel.
    snapshot_levels: (usize, usize),
}

#[test]
fn each_market_s_captured_frames_build_its_own_gap_free_uncrossed_book() {
    let (mut codec, _) = subscribed();
    let specs = venue_specs();
    let mut books = Books::new();
    let mut markets: BTreeMap<InstrumentId, Market> = BTreeMap::new();
    for (i, frame) in capture(CAPTURE).into_iter().enumerate() {
        let (mut sink, mut fx) = (Collect::default(), Effects::new());
        let raw = match &frame {
            Captured::Text(text) => RawFrame::Text(text),
            Captured::Binary(bytes) => RawFrame::Binary(bytes),
        };
        let result = dispatch_market_data(&caps(), |scope| {
            codec.on_frame(raw, scope, &specs, &mut sink, &mut fx)
        });
        assert_eq!(result, Ok(()), "frame {i}");
        assert!(fx.is_empty(), "frame {i}: asked for {fx:?}");
        let events = sink.0;
        let Captured::Binary(bytes) = &frame else {
            // An acknowledgement is consumed and pushes nothing.
            assert!(events.is_empty(), "frame {i}: {events:?}");
            continue;
        };
        let (inst, seq) = frame_seq(bytes, &specs).unwrap().expect("a book frame");
        let book = book_of(inst);
        // Every event of the frame is its own market's, on the channel that market holds.
        for (_, ev) in &events {
            let (ev_inst, ev_book) = match *ev {
                MdEvent::BookSnapshotBegin { inst, book, .. }
                | MdEvent::BookSnapshotEnd { inst, book }
                | MdEvent::Level { inst, book, .. } => (inst, book),
                _ => panic!("frame {i}: not a book event: {ev:?}"),
            };
            assert_eq!((ev_inst, ev_book), (inst, book), "frame {i}");
            books.apply(ev).unwrap();
        }
        match markets.get_mut(&inst) {
            None => {
                assert!(
                    matches!(events.first(), Some((_, MdEvent::BookSnapshotBegin { .. }))),
                    "frame {i}: {inst:?}'s channel opens with a snapshot"
                );
                let side = |want| {
                    let is =
                        |ev: &MdEvent| matches!(ev, MdEvent::Level { side, .. } if *side == want);
                    events.iter().filter(|(_, ev)| is(ev)).count()
                };
                let first = Market {
                    frames: 1,
                    first_seq: seq,
                    last_seq: seq,
                    snapshot_levels: (side(BookSide::Bid), side(BookSide::Ask)),
                };
                markets.insert(inst, first);
            }
            Some(m) => {
                assert_eq!(seq, m.last_seq + 1, "frame {i}: {inst:?} seq_no gap");
                m.frames += 1;
                m.last_seq = seq;
            }
        }
        let touch = books.get(inst, book).unwrap().touch().unwrap();
        let (Some(bid), Some(ask)) = (touch.bid, touch.ask) else {
            panic!("frame {i}: a side of {inst:?} is empty: {touch:?}");
        };
        assert!(bid.px < ask.px, "frame {i} ({inst:?} seq {seq}): crossed");
    }
    assert_eq!(
        markets,
        BTreeMap::from([
            (
                BTC,
                Market {
                    frames: 89,
                    first_seq: 7_687_353_140,
                    last_seq: 7_687_353_228,
                    snapshot_levels: (100, 67),
                },
            ),
            (
                ETH,
                Market {
                    frames: 63,
                    first_seq: 8_106_617_644,
                    last_seq: 8_106_617_706,
                    snapshot_levels: (124, 30),
                },
            ),
            (
                SOL,
                Market {
                    frames: 3,
                    first_seq: 6_182_946_936,
                    last_seq: 6_182_946_938,
                    snapshot_levels: (87, 29),
                },
            ),
        ])
    );
    // No market's frames reached a book on a channel it does not hold.
    for (inst, other) in [
        (BTC, INTERACTIVE_DELTAS),
        (ETH, INTERACTIVE_DELTAS),
        (SOL, DELTAS),
    ] {
        assert!(books.get(inst, other).is_none(), "{inst:?} {other:?}");
    }
}
