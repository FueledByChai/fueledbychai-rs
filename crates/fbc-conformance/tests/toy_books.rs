//! FBC-u1d's done line, sans IO: the conformance toy's `MdCodec` keeps two book channels' levels
//! apart on one connection (Codex r4172231010, record 0014 item 7), pushes nothing from a
//! snapshot frame it cannot decode whole (r4172835744, item 2), refuses a book id outside its
//! declared channels with nothing sent (r4172917339), anchors its `rest_anchor` channel on an
//! HTTP snapshot (r4172835757), and reports a gap as `Health { h: Gap }` naming the channel
//! that broke. Its keepalive reaching a local server is `tests/toy_keepalive.rs`.

use std::collections::BTreeSet;

use fbc_conformance::toy::{
    self, ANCHOR_RETRY, ANCHOR_TIMEOUT, ANCHOR_URL_KEY, ANCHORED_BOOK, BOOK, INST_A, INST_B,
    KEEPALIVE_EVERY, MAX_HELD, MD_URL_KEY, ToyFactory, ToyMd,
};
use fbc_core::{
    BookId, BookSide, ConfigError, ConnKey, DecodeError, Effect, Effects, EndpointPlan, ExchNs,
    Feed, FeedHealth, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag, Inbound,
    InboundSpans, InstrumentId, Keepalive, KeepaliveKind, Lots, MdCodec, MdEvent, MdSink,
    MdTransport, MonoNs, OpKind, RateCharge, RawFrame, StreamId, Subscription, Ticks, TimerTag,
    TrafficClass, VenueConfig, VenueError, VenueFactory, VenueMeta, Via, WallNs, WireSlice,
    WireUrl, dispatch_market_data,
};
use fbc_runtime::{MdBooks, TradingBooks};

use DecodeError::Malformed;

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const STREAM: StreamId = StreamId(4);
const BASE: &str = "http://127.0.0.1:9/anchors";

/// A sink that keeps every event.
#[derive(Default)]
struct Keep(Vec<(VenueMeta, MdEvent)>);

impl MdSink for Keep {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push((meta, ev));
    }
}

impl Keep {
    fn events(&self) -> Vec<MdEvent> {
        self.0.iter().map(|(_, ev)| *ev).collect()
    }
}

fn sub(inst: InstrumentId, book: BookId) -> Subscription {
    Subscription {
        inst,
        feed: Feed::Book(book),
    }
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// A codec of the toy's, a sink and an effect buffer, driven with the toy's specs.
struct Rig {
    md: ToyMd,
    sink: Keep,
    fx: Effects,
}

impl Rig {
    fn new() -> Rig {
        Rig {
            md: ToyMd::with_anchor(STREAM, BASE),
            sink: Keep::default(),
            fx: Effects::new(),
        }
    }

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
    ) -> Result<(), VenueError> {
        self.md.subscribe(add, remove, &toy::specs(), &mut self.fx)
    }

    fn frame(&mut self, text: &str) -> Result<(), DecodeError> {
        let (md, sink, fx) = (&mut self.md, &mut self.sink, &mut self.fx);
        let caps = toy::caps();
        dispatch_market_data(&caps, |scope| {
            md.on_frame(RawFrame::Text(text), scope, &toy::specs(), sink, fx)
        })
    }

    fn http(
        &mut self,
        tag: u64,
        resp: Result<(u16, &str), HttpFailure>,
    ) -> Result<(), DecodeError> {
        let (md, sink, fx) = (&mut self.md, &mut self.sink, &mut self.fx);
        let resp = resp.map(|(status, body)| HttpResponse {
            status,
            headers: &[],
            body: body.as_bytes(),
        });
        let caps = toy::caps();
        dispatch_market_data(&caps, |scope| {
            md.on_http(HttpTag(tag), resp, scope, &toy::specs(), sink, fx)
        })
    }

    fn timer(&mut self, tag: u64) {
        let (sink, fx) = (&mut self.sink, &mut self.fx);
        self.md
            .on_timer(TimerTag(tag), MonoNs(1), WallNs(1), sink, fx);
    }

    /// The events pushed since the last call.
    fn pushed(&mut self) -> Vec<MdEvent> {
        let events = self.sink.events();
        self.sink.0.clear();
        events
    }
}

/// The anchor request for `sym` under `tag`, as the toy asks for it.
fn anchor_get(tag: u64, inst: InstrumentId, sym: &str) -> Effect {
    Effect::Http {
        tag: HttpTag(tag),
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(format!("{BASE}/book/{sym}")),
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout: ANCHOR_TIMEOUT,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Rest, Some(inst)),
    }
}

fn retry(tag: u64) -> Effect {
    Effect::Timer {
        tag: TimerTag(tag),
        after: ANCHOR_RETRY,
    }
}

fn level(inst: InstrumentId, book: BookId, side: BookSide, px: i64, qty: i64) -> MdEvent {
    MdEvent::Level {
        inst,
        book,
        side,
        px: Ticks(px),
        qty: lots(qty),
    }
}

fn gap(inst: InstrumentId, book: BookId) -> MdEvent {
    MdEvent::Health {
        inst,
        feed: Feed::Book(book),
        h: FeedHealth::Gap,
    }
}

const BID: BookSide = BookSide::Bid;
const ASK: BookSide = BookSide::Ask;

// ---------------------------------------------------------------------------------------------
// Two channels on one connection.
// ---------------------------------------------------------------------------------------------

#[test]
fn two_book_channels_on_one_connection_keep_their_levels_apart() {
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, BOOK), sub(INST_A, ANCHORED_BOOK)], &[])
        .unwrap();
    let send = Effect::Send {
        stream: STREAM,
        frame: WireSlice::plain(b"sub|add=TOYA-PERP@0,TOYA-PERP@1".to_vec()),
        rpc: None,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Subscribe, Some(INST_A)),
    };
    assert_eq!(rig.fx.take(), [send, anchor_get(1, INST_A, "TOYA-PERP")]);

    // The same price on both channels, at different sizes.
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3,98:5|ask=101:2")
        .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=6|bid=100:9|ask=")
        .unwrap();
    rig.http(
        1,
        Ok((200, "anchor|sym=TOYA-PERP|seq=5|bid=100:7,99:1|ask=102:4")),
    )
    .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=11|bid=100:0|ask=")
        .unwrap();
    let pushed = rig.pushed();
    let (a, b0, b1) = (INST_A, BOOK, ANCHORED_BOOK);
    assert_eq!(
        pushed,
        [
            MdEvent::BookSnapshotBegin {
                inst: a,
                book: b0,
                epoch: 0
            },
            level(a, b0, BID, 100, 3),
            level(a, b0, BID, 98, 5),
            level(a, b0, ASK, 101, 2),
            MdEvent::BookSnapshotEnd { inst: a, book: b0 },
            MdEvent::BookSnapshotBegin {
                inst: a,
                book: b1,
                epoch: 1
            },
            level(a, b1, BID, 100, 7),
            level(a, b1, BID, 99, 1),
            level(a, b1, ASK, 102, 4),
            MdEvent::BookSnapshotEnd { inst: a, book: b1 },
            // The held delta, after its anchor.
            level(a, b1, BID, 100, 9),
            level(a, b0, BID, 100, 0),
        ]
    );

    // The runtime's books, fed those events from one connection, keep a book per channel.
    let mut books = MdBooks::new(TradingBooks::default());
    let from = ConnKey { conn: 1, epoch: 1 };
    for ev in &pushed {
        books.apply(from, ev).unwrap();
    }
    let at = |book, side, px| books.book(a, book).unwrap().level(side, Ticks(px)).unwrap();
    // 100 removed on BOOK and still 9 on the anchored channel; each side's levels its own.
    assert_eq!(at(b0, BID, 100), Some(lots(0)));
    assert_eq!(at(b1, BID, 100), Some(lots(9)));
    assert_eq!(at(b0, BID, 99), Some(lots(0)));
    assert_eq!(at(b1, BID, 99), Some(lots(1)));
    assert_eq!(at(b0, ASK, 101), Some(lots(2)));
    assert_eq!(at(b1, ASK, 101), Some(lots(0)));
}

// ---------------------------------------------------------------------------------------------
// Whole frames.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_snapshot_frame_it_cannot_decode_whole_pushes_nothing() {
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, BOOK)], &[]).unwrap();
    rig.fx.take();
    let refused = [
        // The last level is bad, after good ones.
        (
            "snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3,99:-1|ask=101:2",
            Malformed("bid"),
        ),
        (
            "snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=101",
            Malformed("ask"),
        ),
        (
            "snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3",
            Malformed("ask"),
        ),
        (
            "snap|sym=TOYA-PERP|book=0|seq=x|bid=|ask=",
            Malformed("seq"),
        ),
        (
            "snap|sym=TOYA-PERP|book=0|seq=10|ts=y|bid=|ask=",
            Malformed("ts"),
        ),
        (
            "snap|sym=NOPE|book=0|seq=10|bid=|ask=",
            DecodeError::UnknownInstrument,
        ),
        (
            "snap|sym=TOYA-PERP|book=2|seq=10|bid=|ask=",
            Malformed("book"),
        ),
        (
            "snap|sym=TOYA-PERP|book=1|seq=10|bid=|ask=",
            Malformed("an anchored book's snapshot comes over HTTP"),
        ),
        (
            "book|sym=TOYA-PERP|book=0|seq=10|bid=|ask=",
            Malformed("kind"),
        ),
        (
            "snap|sym=TOYA-PERP|book=0|seq=10|bid=|ask=\nsnap",
            Malformed("one record per frame"),
        ),
    ];
    for (frame, why) in refused {
        assert_eq!(rig.frame(frame), Err(why), "{frame}");
    }
    let caps = toy::caps();
    let binary = dispatch_market_data(&caps, |scope| {
        let f = RawFrame::Binary(b"snap");
        rig.md
            .on_frame(f, scope, &toy::specs(), &mut rig.sink, &mut rig.fx)
    });
    assert_eq!(binary, Err(Malformed("binary frame")));
    assert!(rig.pushed().is_empty());
    assert!(rig.fx.is_empty());

    // Nothing was anchored: a delta is dropped until a snapshot decodes whole.
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=11|bid=100:1|ask=")
        .unwrap();
    assert!(rig.pushed().is_empty());
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=10|ts=7|bid=100:3|ask=")
        .unwrap();
    assert_eq!(rig.sink.0[0].0.exch_ts, Some(ExchNs(7)));
    assert_eq!(rig.pushed().len(), 3);

    // A delta it cannot decode whole pushes nothing and uses up no sequence.
    let bad = rig.frame("delta|sym=TOYA-PERP|book=0|seq=11|bid=100:1,99:x|ask=");
    assert_eq!(bad, Err(Malformed("bid")));
    assert!(rig.pushed().is_empty());
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=11|bid=100:1|ask=")
        .unwrap();
    assert_eq!(rig.pushed(), [level(INST_A, BOOK, BID, 100, 1)]);
    assert!(rig.fx.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Refused subscriptions.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_book_id_outside_the_declared_channels_is_refused_with_nothing_sent() {
    assert_eq!(toy::caps().md.books.len(), 2);
    let mut rig = Rig::new();
    let outside = sub(INST_A, BookId(2));
    let refused = rig.subscribe(&[sub(INST_A, BOOK), outside], &[]);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(outside)));
    let trades = Subscription {
        inst: INST_A,
        feed: Feed::Trades,
    };
    let refused = rig.subscribe(&[], &[trades]);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(trades)));
    let unknown = sub(InstrumentId::new(99), BOOK);
    let refused = rig.subscribe(&[unknown], &[]);
    assert_eq!(refused, Err(VenueError::UnknownInstrument(unknown.inst)));
    assert!(rig.fx.is_empty());
    // Nothing of the refused call was taken: its declared channel is not subscribed.
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=1|bid=100:1|ask=")
        .unwrap();
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.subscribe(&[], &[]), Ok(()));
    assert!(rig.fx.is_empty());

    // The factory's codec has no anchor URL (FBC-ja3): the anchored channel is refused as
    // configuration, with nothing sent.
    let cfg = VenueConfig::new();
    let ep = EndpointPlan {
        stream: STREAM,
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1/md"),
        },
        subs: Vec::new(),
    };
    let mut md = ToyFactory.md_codec(&cfg, &ep);
    let mut fx = Effects::new();
    let refused = md.subscribe(
        &[sub(INST_A, BOOK), sub(INST_A, ANCHORED_BOOK)],
        &[],
        &toy::specs(),
        &mut fx,
    );
    let Err(VenueError::Config(ConfigError::Invalid { key, reason })) = refused else {
        panic!("{refused:?}");
    };
    assert_eq!(key, ANCHOR_URL_KEY);
    assert!(reason.contains("FBC-ja3"), "{reason}");
    assert!(fx.is_empty());
    md.on_open(&mut fx);
    assert!(fx.is_empty());

    // The factory plans nothing for nothing, refuses an undeclared channel as unsupported and a
    // declared one as configuration, its market-data URL not modelled until FBC-ja3.
    let specs = toy::specs();
    let plan = |subs: &[Subscription]| {
        ToyFactory.plan_md(&cfg, &specs, &BTreeSet::from_iter(subs.iter().copied()))
    };
    assert!(matches!(plan(&[]), Ok(p) if p.is_empty()));
    assert_eq!(
        plan(&[sub(INST_A, BOOK), outside]),
        Err(VenueError::UnsupportedFeed(outside))
    );
    let Err(VenueError::Config(ConfigError::Invalid { key, .. })) = plan(&[sub(INST_B, BOOK)])
    else {
        panic!("planned");
    };
    assert_eq!(key, MD_URL_KEY);
}

#[test]
fn an_unknown_instrument_is_named_before_any_feed_in_either_list() {
    // Codex r4216704345: the codec checks every instrument, added or removed, before any feed,
    // so a missing one is named even behind a known instrument's undeclared feed.
    let trades = Subscription {
        inst: INST_A,
        feed: Feed::Trades,
    };
    let unknown = sub(InstrumentId::new(99), BOOK);
    let mut rig = Rig::new();
    let calls: [(&[Subscription], &[Subscription]); 4] = [
        (&[trades, unknown], &[]),
        (&[trades], &[unknown]),
        (&[], &[trades, unknown]),
        (&[sub(INST_A, BookId(2))], &[unknown]),
    ];
    for (add, remove) in calls {
        let refused = rig.subscribe(add, remove);
        assert_eq!(
            refused,
            Err(VenueError::UnknownInstrument(unknown.inst)),
            "{add:?} {remove:?}"
        );
    }
    assert!(rig.fx.is_empty());
}

// ---------------------------------------------------------------------------------------------
// The REST anchor.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_rest_anchored_book_is_anchored_on_its_http_snapshot_retrying_until_it_decodes() {
    assert!(toy::caps().md.books[usize::from(ANCHORED_BOOK.0)].rest_anchor);
    assert!(!toy::caps().md.books[usize::from(BOOK.0)].rest_anchor);
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, ANCHORED_BOOK)], &[]).unwrap();
    assert_eq!(rig.fx.as_slice()[1..], [anchor_get(1, INST_A, "TOYA-PERP")]);
    rig.fx.take();
    // Deltas are held while the anchor is asked for.
    for seq in 4..=6 {
        let d = format!("delta|sym=TOYA-PERP|book=1|seq={seq}|bid=100:{seq}|ask=");
        rig.frame(&d).unwrap();
    }
    assert!(rig.pushed().is_empty());

    // A failure, a status other than 200 and a body it cannot decode each ask again after the
    // retry; a superseded tag is ignored.
    assert_eq!(rig.http(1, Err(HttpFailure::TimedOut)), Ok(()));
    assert_eq!(rig.fx.take(), [retry(1)]);
    rig.timer(1);
    assert_eq!(rig.fx.take(), [anchor_get(2, INST_A, "TOYA-PERP")]);
    rig.timer(1);
    assert_eq!(
        rig.http(1, Ok((200, "anchor|sym=TOYA-PERP|seq=3|bid=|ask="))),
        Ok(())
    );
    assert!(rig.fx.is_empty());
    assert_eq!(rig.http(2, Ok((503, ""))), Ok(()));
    assert_eq!(rig.fx.take(), [retry(2)]);
    rig.timer(2);
    assert_eq!(rig.fx.take(), [anchor_get(3, INST_A, "TOYA-PERP")]);
    for (body, why) in [
        ("anchor|sym=TOYA-PERP|seq=3|bid=100:1", Malformed("ask")),
        ("anchor|sym=TOYB-PERP|seq=3|bid=|ask=", Malformed("anchor")),
        ("snap|sym=TOYA-PERP|seq=3|bid=|ask=", Malformed("anchor")),
    ] {
        assert_eq!(rig.http(3, Ok((200, body))), Err(why), "{body}");
        assert_eq!(rig.fx.take(), [retry(3)]);
    }
    let raw = HttpResponse {
        status: 200,
        headers: &[],
        body: &[0xff],
    };
    let caps = toy::caps();
    let not_utf8 = dispatch_market_data(&caps, |scope| {
        rig.md.on_http(
            HttpTag(3),
            Ok(raw),
            scope,
            &toy::specs(),
            &mut rig.sink,
            &mut rig.fx,
        )
    });
    assert_eq!(not_utf8, Err(Malformed("body")));
    assert_eq!(rig.fx.take(), [retry(3)]);
    assert!(rig.pushed().is_empty());

    // The anchor at 4: the held delta 4 is dropped, 5 and 6 follow it, and 7 is live.
    rig.timer(3);
    assert_eq!(rig.fx.take(), [anchor_get(4, INST_A, "TOYA-PERP")]);
    rig.http(
        4,
        Ok((200, "anchor|sym=TOYA-PERP|seq=4|bid=100:4|ask=101:1")),
    )
    .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=7|bid=100:7|ask=")
        .unwrap();
    let (a, b1) = (INST_A, ANCHORED_BOOK);
    assert_eq!(
        rig.pushed(),
        [
            MdEvent::BookSnapshotBegin {
                inst: a,
                book: b1,
                epoch: 0
            },
            level(a, b1, BID, 100, 4),
            level(a, b1, ASK, 101, 1),
            MdEvent::BookSnapshotEnd { inst: a, book: b1 },
            level(a, b1, BID, 100, 5),
            level(a, b1, BID, 100, 6),
            level(a, b1, BID, 100, 7),
        ]
    );
    assert!(rig.fx.is_empty());
    // An answer to a tag no channel waits on is ignored, and so is its timer.
    assert_eq!(
        rig.http(4, Ok((200, "anchor|sym=TOYA-PERP|seq=9|bid=|ask="))),
        Ok(())
    );
    rig.timer(4);
    assert!(rig.pushed().is_empty() && rig.fx.is_empty());
}

#[test]
fn an_anchor_older_than_a_held_delta_or_one_past_the_held_bound_is_asked_again() {
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_B, ANCHORED_BOOK)], &[]).unwrap();
    rig.fx.take();
    // Delta 9 was missed: an anchor at 7 cannot reach 10.
    rig.frame("delta|sym=TOYB-PERP|book=1|seq=10|bid=50:1|ask=")
        .unwrap();
    rig.http(1, Ok((200, "anchor|sym=TOYB-PERP|seq=8|bid=50:2|ask=")))
        .unwrap();
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.fx.take(), [anchor_get(2, INST_B, "TOYB-PERP")]);

    // The held bound: one delta more than it holds asks again rather than grow.
    for seq in 0..=MAX_HELD {
        let d = format!("delta|sym=TOYB-PERP|book=1|seq={}|bid=50:1|ask=", 20 + seq);
        rig.frame(&d).unwrap();
    }
    assert_eq!(rig.fx.take(), [anchor_get(3, INST_B, "TOYB-PERP")]);
    // The superseded anchor no longer anchors anything.
    rig.http(2, Ok((200, "anchor|sym=TOYB-PERP|seq=19|bid=50:2|ask=")))
        .unwrap();
    assert!(rig.pushed().is_empty() && rig.fx.is_empty());

    // An anchor answered after its subscription was removed pushes nothing.
    rig.subscribe(&[], &[sub(INST_B, ANCHORED_BOOK)]).unwrap();
    assert_eq!(rig.fx.len(), 1);
    rig.fx.take();
    rig.http(3, Ok((200, "anchor|sym=TOYB-PERP|seq=19|bid=50:2|ask=")))
        .unwrap();
    rig.frame("delta|sym=TOYB-PERP|book=1|seq=20|bid=50:1|ask=")
        .unwrap();
    assert!(rig.pushed().is_empty() && rig.fx.is_empty());
}

#[test]
fn the_delta_past_the_held_bound_is_held_for_the_anchor_asked_again() {
    // Codex r4203051298: the delta that overflows the bound is the first the replacement anchor
    // must reach, so an anchor older than it is asked for again, never pushed as live.
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_B, ANCHORED_BOOK)], &[]).unwrap();
    rig.fx.take();
    let over = 20 + MAX_HELD as u64;
    for seq in 20..=over {
        let d = format!(
            "delta|sym=TOYB-PERP|book=1|seq={seq}|bid=50:{}|ask=",
            seq % 7 + 1
        );
        rig.frame(&d).unwrap();
    }
    assert_eq!(rig.fx.take(), [anchor_get(2, INST_B, "TOYB-PERP")]);
    // An anchor two before the overflowing delta missed the one between: asked again.
    let stale = format!("anchor|sym=TOYB-PERP|seq={}|bid=50:9|ask=", over - 2);
    rig.http(2, Ok((200, &stale))).unwrap();
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.fx.take(), [anchor_get(3, INST_B, "TOYB-PERP")]);
    // The overflowing delta is still held for the anchor after that: one just before it is
    // followed by it.
    let fresh = format!("anchor|sym=TOYB-PERP|seq={}|bid=50:9|ask=", over - 1);
    rig.frame(&format!(
        "delta|sym=TOYB-PERP|book=1|seq={}|bid=51:1|ask=",
        over + 1
    ))
    .unwrap();
    rig.http(3, Ok((200, &fresh))).unwrap();
    let b = (INST_B, ANCHORED_BOOK);
    assert_eq!(
        rig.pushed(),
        [
            MdEvent::BookSnapshotBegin {
                inst: b.0,
                book: b.1,
                epoch: 0
            },
            level(b.0, b.1, BID, 50, 9),
            MdEvent::BookSnapshotEnd {
                inst: b.0,
                book: b.1
            },
            level(b.0, b.1, BID, 50, (over % 7 + 1) as i64),
            level(b.0, b.1, BID, 51, 1),
        ]
    );
    assert!(rig.fx.is_empty());
}

#[test]
fn an_anchor_at_the_last_sequence_is_pushed_and_the_delta_after_it_is_a_gap() {
    // Codex r4203051299: no sequence follows u64::MAX, so nothing chains onto such an anchor;
    // nothing overflows on the way.
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, ANCHORED_BOOK)], &[]).unwrap();
    rig.fx.take();
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=0|bid=100:1|ask=")
        .unwrap();
    let last = format!("anchor|sym=TOYA-PERP|seq={}|bid=100:2|ask=", u64::MAX);
    rig.http(1, Ok((200, &last))).unwrap();
    let a = (INST_A, ANCHORED_BOOK);
    assert_eq!(
        rig.pushed(),
        [
            MdEvent::BookSnapshotBegin {
                inst: a.0,
                book: a.1,
                epoch: 0
            },
            level(a.0, a.1, BID, 100, 2),
            MdEvent::BookSnapshotEnd {
                inst: a.0,
                book: a.1
            },
        ]
    );
    assert!(rig.fx.is_empty());
    // A delta at 0 does not follow u64::MAX: a gap, and the anchor asked for again.
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=0|bid=100:3|ask=")
        .unwrap();
    assert_eq!(rig.pushed(), [gap(INST_A, ANCHORED_BOOK)]);
    assert_eq!(rig.fx.take(), [anchor_get(2, INST_A, "TOYA-PERP")]);
}

#[test]
fn an_anchor_behind_what_the_channel_already_applied_is_asked_again() {
    // Codex r4216139627: a delta repeated or going back is a gap, and the next anchor must not
    // move the channel back behind the sequence it had reached.
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, ANCHORED_BOOK)], &[]).unwrap();
    rig.fx.take();
    rig.http(1, Ok((200, "anchor|sym=TOYA-PERP|seq=10|bid=100:1|ask=")))
        .unwrap();
    assert_eq!(rig.pushed().len(), 3);
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=8|bid=100:2|ask=")
        .unwrap();
    assert_eq!(rig.pushed(), [gap(INST_A, ANCHORED_BOOK)]);
    assert_eq!(rig.fx.take(), [anchor_get(2, INST_A, "TOYA-PERP")]);
    // An anchor at 7 would chain onto the held 8 but is behind 10: asked again.
    rig.http(2, Ok((200, "anchor|sym=TOYA-PERP|seq=7|bid=100:3|ask=")))
        .unwrap();
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.fx.take(), [anchor_get(3, INST_A, "TOYA-PERP")]);
    // The floor outlives a failed anchor and its retry.
    rig.http(3, Err(HttpFailure::TimedOut)).unwrap();
    assert_eq!(rig.fx.take(), [retry(3)]);
    rig.timer(3);
    assert_eq!(rig.fx.take(), [anchor_get(4, INST_A, "TOYA-PERP")]);
    rig.http(4, Ok((200, "anchor|sym=TOYA-PERP|seq=9|bid=100:3|ask=")))
        .unwrap();
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.fx.take(), [anchor_get(5, INST_A, "TOYA-PERP")]);
    // One at 10 is live again, and 11 follows it.
    rig.http(5, Ok((200, "anchor|sym=TOYA-PERP|seq=10|bid=100:4|ask=")))
        .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=11|bid=100:5|ask=")
        .unwrap();
    let a = (INST_A, ANCHORED_BOOK);
    assert_eq!(
        rig.pushed(),
        [
            MdEvent::BookSnapshotBegin {
                inst: a.0,
                book: a.1,
                epoch: 1
            },
            level(a.0, a.1, BID, 100, 4),
            MdEvent::BookSnapshotEnd {
                inst: a.0,
                book: a.1
            },
            level(a.0, a.1, BID, 100, 5),
        ]
    );
    assert!(rig.fx.is_empty());
}

#[test]
fn the_held_high_water_mark_outlives_the_held_bound() {
    // Codex r4216289348: a repeated delta overflowing the bound is held, and the anchor asked
    // again must still reach every delta already seen, the highest held before it included.
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_B, ANCHORED_BOOK)], &[]).unwrap();
    rig.fx.take();
    let high = 20 + MAX_HELD as u64 - 1;
    for seq in 20..=high {
        rig.frame(&format!(
            "delta|sym=TOYB-PERP|book=1|seq={seq}|bid=50:1|ask="
        ))
        .unwrap();
    }
    assert!(rig.fx.is_empty());
    rig.frame("delta|sym=TOYB-PERP|book=1|seq=25|bid=50:2|ask=")
        .unwrap();
    assert_eq!(rig.fx.take(), [anchor_get(2, INST_B, "TOYB-PERP")]);
    // An anchor at 24 chains onto the held 25 but is behind 26..=high: asked again.
    rig.http(2, Ok((200, "anchor|sym=TOYB-PERP|seq=24|bid=50:9|ask=")))
        .unwrap();
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.fx.take(), [anchor_get(3, INST_B, "TOYB-PERP")]);
    // One at the highest delta seen is live, and the next follows it.
    let fresh = format!("anchor|sym=TOYB-PERP|seq={high}|bid=50:9|ask=");
    rig.http(3, Ok((200, &fresh))).unwrap();
    rig.frame(&format!(
        "delta|sym=TOYB-PERP|book=1|seq={}|bid=51:1|ask=",
        high + 1
    ))
    .unwrap();
    let b = (INST_B, ANCHORED_BOOK);
    assert_eq!(
        rig.pushed(),
        [
            MdEvent::BookSnapshotBegin {
                inst: b.0,
                book: b.1,
                epoch: 0
            },
            level(b.0, b.1, BID, 50, 9),
            MdEvent::BookSnapshotEnd {
                inst: b.0,
                book: b.1
            },
            level(b.0, b.1, BID, 51, 1),
        ]
    );
    assert!(rig.fx.is_empty());
}

#[test]
fn a_book_snapshot_behind_what_the_channel_already_applied_is_dropped() {
    // Codex r4216289341: a snapshot frame may not move BOOK back behind the sequence it had
    // reached, after a gap or while live.
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, BOOK)], &[]).unwrap();
    rig.fx.take();
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=10|bid=100:1|ask=")
        .unwrap();
    assert_eq!(rig.pushed().len(), 3);
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=12|bid=100:2|ask=")
        .unwrap();
    assert_eq!(rig.pushed(), [gap(INST_A, BOOK)]);
    // A snapshot at 7 is behind 10: dropped, and the channel still waits.
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=7|bid=100:3|ask=")
        .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=8|bid=100:4|ask=")
        .unwrap();
    assert!(rig.pushed().is_empty());
    // The floor outlives a dropped snapshot; one at 10 is live again, and 11 follows it.
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=9|bid=100:3|ask=")
        .unwrap();
    assert!(rig.pushed().is_empty());
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=10|bid=100:5|ask=")
        .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=11|bid=100:6|ask=")
        .unwrap();
    let a = (INST_A, BOOK);
    assert_eq!(
        rig.pushed(),
        [
            MdEvent::BookSnapshotBegin {
                inst: a.0,
                book: a.1,
                epoch: 1
            },
            level(a.0, a.1, BID, 100, 5),
            MdEvent::BookSnapshotEnd {
                inst: a.0,
                book: a.1
            },
            level(a.0, a.1, BID, 100, 6),
        ]
    );
    // Live at 11, a snapshot at 10 is behind it too: dropped, and 12 still follows 11.
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=10|bid=100:7|ask=")
        .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=12|bid=100:8|ask=")
        .unwrap();
    assert_eq!(rig.pushed(), [level(INST_A, BOOK, BID, 100, 8)]);
    assert!(rig.fx.is_empty());
}

#[test]
fn a_record_deeper_than_the_declared_depth_is_refused_whole() {
    // Codex r4216289357: no snapshot, anchor or delta carries more levels per side than the
    // channel's declared max_depth.
    let depth = usize::from(toy::caps().md.books[usize::from(BOOK.0)].max_depth);
    assert_eq!(
        depth,
        usize::from(toy::caps().md.books[usize::from(ANCHORED_BOOK.0)].max_depth)
    );
    let side = |n: usize| {
        (0..n)
            .map(|i| format!("{}:1", 100 + i))
            .collect::<Vec<_>>()
            .join(",")
    };
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, BOOK), sub(INST_A, ANCHORED_BOOK)], &[])
        .unwrap();
    rig.fx.take();
    let deep = format!(
        "snap|sym=TOYA-PERP|book=0|seq=1|bid={}|ask=",
        side(depth + 1)
    );
    assert_eq!(rig.frame(&deep), Err(Malformed("depth")));
    let deep = format!(
        "snap|sym=TOYA-PERP|book=0|seq=1|bid=|ask={}",
        side(depth + 1)
    );
    assert_eq!(rig.frame(&deep), Err(Malformed("depth")));
    assert!(rig.pushed().is_empty());
    // An anchor deeper than declared is asked for again after the retry.
    let deep = format!("anchor|sym=TOYA-PERP|seq=1|bid=|ask={}", side(depth + 1));
    assert_eq!(rig.http(1, Ok((200, &deep))), Err(Malformed("depth")));
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.fx.take(), [retry(1)]);
    // At the declared depth on both sides, a snapshot is pushed whole; a delta past it is not.
    let full = format!(
        "snap|sym=TOYA-PERP|book=0|seq=1|bid={}|ask={}",
        side(depth),
        side(depth)
    );
    rig.frame(&full).unwrap();
    assert_eq!(rig.pushed().len(), 2 * depth + 2);
    let deep = format!(
        "delta|sym=TOYA-PERP|book=0|seq=2|bid={}|ask=",
        side(depth + 1)
    );
    assert_eq!(rig.frame(&deep), Err(Malformed("depth")));
    assert!(rig.pushed().is_empty());
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=2|bid=100:2|ask=")
        .unwrap();
    assert_eq!(rig.pushed(), [level(INST_A, BOOK, BID, 100, 2)]);
    assert!(rig.fx.is_empty());
}

/// Applies the events pushed since the last call to `books`, from one connection, and returns
/// them.
fn feed(books: &mut MdBooks, rig: &mut Rig) -> Vec<MdEvent> {
    let pushed = rig.pushed();
    for ev in &pushed {
        books.apply(ConnKey { conn: 1, epoch: 1 }, ev).unwrap();
    }
    pushed
}

// ---------------------------------------------------------------------------------------------
// Gaps.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_gap_reports_health_naming_the_book_that_broke_and_leaves_the_other_alone() {
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, BOOK), sub(INST_A, ANCHORED_BOOK)], &[])
        .unwrap();
    rig.fx.take();
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=10|bid=100:3|ask=")
        .unwrap();
    rig.http(1, Ok((200, "anchor|sym=TOYA-PERP|seq=5|bid=100:7|ask=")))
        .unwrap();
    let mut books = MdBooks::new(TradingBooks::default());
    feed(&mut books, &mut rig);

    // 11 is missed on BOOK: the gap names it, with the frame's sequence.
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=12|bid=100:4|ask=")
        .unwrap();
    assert_eq!(rig.sink.0[0].0.venue_seq, Some(12));
    assert_eq!(feed(&mut books, &mut rig), [gap(INST_A, BOOK)]);
    assert!(rig.fx.is_empty());
    // BOOK drops its deltas until its next snapshot; the anchored channel goes on.
    rig.frame("delta|sym=TOYA-PERP|book=0|seq=13|bid=100:5|ask=")
        .unwrap();
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=6|bid=100:8|ask=")
        .unwrap();
    assert_eq!(
        feed(&mut books, &mut rig),
        [level(INST_A, ANCHORED_BOOK, BID, 100, 8)]
    );
    let book = |books: &MdBooks, b| books.book(INST_A, b).unwrap().level(BID, Ticks(100));
    assert!(book(&books, BOOK).is_err());
    assert_eq!(book(&books, ANCHORED_BOOK), Ok(Some(lots(8))));
    rig.frame("snap|sym=TOYA-PERP|book=0|seq=20|bid=100:2|ask=")
        .unwrap();
    feed(&mut books, &mut rig);
    assert_eq!(book(&books, BOOK), Ok(Some(lots(2))));

    // A gap on the anchored channel names it too, and asks for a new anchor at once.
    rig.frame("delta|sym=TOYA-PERP|book=1|seq=8|bid=100:9|ask=")
        .unwrap();
    assert_eq!(feed(&mut books, &mut rig), [gap(INST_A, ANCHORED_BOOK)]);
    assert_eq!(rig.fx.take(), [anchor_get(2, INST_A, "TOYA-PERP")]);
    assert!(book(&books, ANCHORED_BOOK).is_err());
    assert_eq!(book(&books, BOOK), Ok(Some(lots(2))));
    // The delta that broke it is held for the new anchor: one at 6 misses 7 and is asked for
    // again; one at 7 is followed by it.
    rig.http(2, Ok((200, "anchor|sym=TOYA-PERP|seq=6|bid=100:8|ask=")))
        .unwrap();
    assert!(rig.pushed().is_empty());
    assert_eq!(rig.fx.take(), [anchor_get(3, INST_A, "TOYA-PERP")]);
    rig.http(3, Ok((200, "anchor|sym=TOYA-PERP|seq=7|bid=100:1|ask=")))
        .unwrap();
    feed(&mut books, &mut rig);
    assert_eq!(book(&books, ANCHORED_BOOK), Ok(Some(lots(9))));
}

#[test]
fn every_market_data_request_is_counted_by_a_declared_limit() {
    // Codex r4216139636: the subscribe frame, the anchor's GET and the keepalive each fall in
    // a bucket the toy declares, so a session's limiter throttles them.
    let limits = toy::caps().limits;
    let counted = |charge: &RateCharge, via| limits.iter().any(|l| l.counts(charge, via));
    let mut rig = Rig::new();
    rig.subscribe(&[sub(INST_A, BOOK), sub(INST_A, ANCHORED_BOOK)], &[])
        .unwrap();
    let fx = rig.fx.take();
    assert_eq!(fx.len(), 2);
    for effect in &fx {
        match effect {
            Effect::Send { charge, .. } => assert!(counted(charge, Via::Frame), "{effect:?}"),
            Effect::Http { charge, .. } => assert!(counted(charge, Via::Http), "{effect:?}"),
            other => panic!("{other:?}"),
        }
    }
    let ping = rig.md.keepalive().unwrap();
    assert!(counted(&ping.charge, Via::Frame));
}

// ---------------------------------------------------------------------------------------------
// What it declares.
// ---------------------------------------------------------------------------------------------

#[test]
fn it_declares_a_keepalive_frame_and_names_no_credential() {
    let md = ToyMd::new(STREAM);
    let ping = Keepalive {
        interval: KEEPALIVE_EVERY,
        kind: KeepaliveKind::Frame(WireSlice::plain(b"ping".to_vec())),
        charge: RateCharge::one(OpKind::Control, None),
    };
    assert_eq!(md.keepalive(), Some(ping));
    let frame = RawFrame::Text("snap|sym=TOYA-PERP");
    assert_eq!(md.redact_inbound(Inbound::Frame(frame)), InboundSpans::NONE);
}
