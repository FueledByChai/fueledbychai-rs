//! FBC-nij's done line: the runtime keeps one book per (instrument, book channel). Interleaved
//! snapshots and deltas of `Feed::Book(BookId(0))` and `Feed::Book(BookId(1))` of one
//! instrument, from one connection, build two separate books (a level or a gap on one leaves
//! the other unchanged), and the book exposed as the instrument's trading book is the configured
//! channel's, never the other one, even while the other is the only valid book.

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use common::ScriptedWs;
use common::toy::{self, ToyVenue};
use fbc_book::{Applied, BookError, BookState, Top, Touch};
use fbc_core::{
    BookId, BookSide, EndpointPlan, Envelope, Feed, FeedHealth, InstrumentId, Lots, Lvl, MdEvent,
    MdTransport, Ticks, VenueConfig, WireUrl,
};
use fbc_runtime::{
    BookKeeper, Connector, IngestClock, Liveness, MdBooks, MdHandler, MdSession, MdSessionConfig,
    ProxyConfig, ReconnectPacing, TradingBookConflict, TradingBooks,
};

const A: u32 = 1;
const PUBLIC: BookId = BookId(0);
const INTERACTIVE: BookId = BookId(1);

fn inst(id: u32) -> InstrumentId {
    InstrumentId::new(id)
}

fn lvl(px: i64, qty: i64) -> Lvl {
    Lvl {
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// What the keeper's handler saw after each event: the event, both books' canonical bytes
/// (`None` before the book's first event), the trading book's touch, and all the books.
struct After {
    body: MdEvent,
    public: Option<Vec<u8>>,
    interactive: Option<Vec<u8>>,
    trading: Option<Result<Touch, BookError>>,
    books: MdBooks,
}

type Seen = Rc<RefCell<Vec<After>>>;

fn bytes(books: &MdBooks, book: BookId) -> Option<Vec<u8>> {
    books.book(inst(A), book).map(|b| b.canonical_bytes())
}

/// The channel an event is about, if it is about a book.
fn channel(ev: &MdEvent) -> Option<BookId> {
    match *ev {
        MdEvent::BookSnapshotBegin { book, .. }
        | MdEvent::BookSnapshotEnd { book, .. }
        | MdEvent::Level { book, .. }
        | MdEvent::Window { book, .. }
        | MdEvent::Health {
            feed: Feed::Book(book),
            ..
        } => Some(book),
        _ => None,
    }
}

#[tokio::test]
async fn two_book_channels_of_one_instrument_on_one_connection_stay_separate_and_the_configured_one_trades()
 {
    let mut server = ScriptedWs::start().await;
    let venue = ToyVenue::leak();
    let config = MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(server.url()),
            },
            subs: vec![toy::book(A, PUBLIC), toy::book(A, INTERACTIVE)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: 3,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
    };
    // The interactive channel is the configured trading book of instrument A.
    let trading = TradingBooks::new([(inst(A), INTERACTIVE)]).unwrap();
    let seen = Seen::default();
    let log = seen.clone();
    let keeper = BookKeeper::new(
        MdBooks::new(trading),
        move |env: Envelope<MdEvent>, books: &MdBooks| {
            log.borrow_mut().push(After {
                body: env.body,
                public: bytes(books, PUBLIC),
                interactive: bytes(books, INTERACTIVE),
                trading: books.trading_book(inst(A)).map(|b| b.touch()),
                books: books.clone(),
            })
        },
    );
    let (mut session, control) = MdSession::new(config, keeper).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,1");
        assert_eq!(peer.recv().await, "sub|add=A,A");
        for frame in [
            // Both channels' snapshots, interleaved: a level of one arrives while the other's
            // replacement is in progress.
            "begin|sym=A|book=0|epoch=1|seq=10",
            "lvl|sym=A|book=0|side=bid|px=100|qty=5|seq=11",
            "begin|sym=A|book=1|epoch=1|seq=50",
            "lvl|sym=A|book=1|side=bid|px=100|qty=2|seq=51",
            "lvl|sym=A|book=0|side=ask|px=101|qty=4|seq=12",
            "end|sym=A|book=0|seq=13",
            "lvl|sym=A|book=1|side=ask|px=102|qty=7|seq=52",
            "end|sym=A|book=1|seq=53",
            // Deltas on each.
            "lvl|sym=A|book=0|side=bid|px=100|qty=6|seq=14",
            "lvl|sym=A|book=1|side=ask|px=101|qty=1|seq=54",
            // A skipped seq on the public channel: a gap on it alone.
            "lvl|sym=A|book=0|side=ask|px=101|qty=0|seq=16",
            "lvl|sym=A|book=1|side=bid|px=99|qty=3|seq=55",
            // Dropped by the codec: the public channel waits for its next snapshot.
            "lvl|sym=A|book=0|side=bid|px=98|qty=1|seq=17",
            // Malformed: a decode error, nothing pushed.
            "lvl|sym=A|book=1|side=up|px=99|qty=1|seq=56",
            "begin|sym=A|book=0|epoch=2|seq=20",
            "lvl|sym=A|book=0|side=bid|px=99|qty=9|seq=21",
            "end|sym=A|book=0|seq=22",
            // A skipped seq on the trading channel: a gap on it alone.
            "lvl|sym=A|book=1|side=bid|px=100|qty=0|seq=57",
        ] {
            peer.send(frame);
        }
        while watch.borrow().len() < 16 {
            tokio::time::sleep(ms(2)).await;
        }
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();

    let seen = seen.borrow();
    let gap = |book| MdEvent::Health {
        inst: inst(A),
        feed: Feed::Book(book),
        h: FeedHealth::Gap,
    };
    let bodies: Vec<_> = seen.iter().map(|a| a.body).collect();
    assert_eq!(bodies.len(), 16);
    assert_eq!(bodies[10], gap(PUBLIC));
    assert_eq!(bodies[15], gap(INTERACTIVE));

    // Every event changed at most its own channel's book: the other's bytes are those it had
    // before the event.
    let mut before = (None, None);
    for after in seen.iter() {
        match channel(&after.body) {
            Some(PUBLIC) => assert_eq!(after.interactive, before.1, "{:?}", after.body),
            Some(INTERACTIVE) => assert_eq!(after.public, before.0, "{:?}", after.body),
            other => panic!("not a book event of the two channels: {other:?}"),
        }
        before = (after.public.clone(), after.interactive.clone());
    }
    // The public channel's events changed it, the interactive channel's changed that one.
    for (i, after) in seen.iter().enumerate().skip(1) {
        let changed = match channel(&after.body) {
            Some(PUBLIC) => after.public != seen[i - 1].public,
            _ => after.interactive != seen[i - 1].interactive,
        };
        assert!(changed, "event {i} {:?} changed nothing", after.body);
    }

    // While the public channel was gapped (after event 11) the trading book was the
    // interactive one, valid, with its own levels: never the public channel's.
    assert_eq!(
        seen[11].trading,
        Some(Ok(Touch {
            bid: Some(lvl(100, 2)),
            ask: Some(lvl(101, 1)),
        }))
    );
    // Before the interactive snapshot completed, the trading book could not be read even though
    // the public one was valid by then (event 5).
    assert_eq!(
        seen[5].trading,
        Some(Err(BookError::NotValid(BookState::AwaitingSnapshot)))
    );
    // After the trading channel's gap, it is invalid while the public channel is valid again.
    assert_eq!(
        seen[15].trading,
        Some(Err(BookError::NotValid(BookState::Gapped)))
    );

    let books = &seen[15].books;
    let public = books.book(inst(A), PUBLIC).unwrap();
    assert_eq!(public.state(), BookState::Valid { epoch: 2 });
    assert_eq!(
        public.top(15).unwrap(),
        Top {
            bids: vec![lvl(99, 9)],
            asks: vec![],
        }
    );
    let interactive = books.book(inst(A), INTERACTIVE).unwrap();
    assert_eq!(interactive.state(), BookState::Gapped);
    assert_eq!(books.trading_book_id(inst(A)), Some(INTERACTIVE));
    assert!(std::ptr::eq(
        books.trading_book(inst(A)).unwrap(),
        interactive
    ));
    assert_eq!(books.refused(), 0);
    assert_eq!(session.counters().decode_errors, 1);
}

fn level(book: BookId, side: BookSide, px: i64, qty: i64) -> MdEvent {
    MdEvent::Level {
        inst: inst(A),
        book,
        side,
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

fn snapshot(books: &mut MdBooks, book: BookId, epoch: u32, levels: &[(BookSide, i64, i64)]) {
    let begin = MdEvent::BookSnapshotBegin {
        inst: inst(A),
        book,
        epoch,
    };
    assert_eq!(books.apply(&begin), Ok(Applied::Changed));
    for &(side, px, qty) in levels {
        assert_eq!(
            books.apply(&level(book, side, px, qty)),
            Ok(Applied::Changed)
        );
    }
    let end = MdEvent::BookSnapshotEnd {
        inst: inst(A),
        book,
    };
    assert_eq!(books.apply(&end), Ok(Applied::Changed));
}

#[test]
fn a_window_on_one_channel_bounds_only_that_channel_and_other_feeds_change_no_book() {
    let mut books = MdBooks::new(TradingBooks::new([(inst(A), PUBLIC)]).unwrap());
    let both = [(BookSide::Bid, 90, 1), (BookSide::Ask, 110, 1)];
    snapshot(&mut books, PUBLIC, 1, &both);
    snapshot(&mut books, INTERACTIVE, 1, &both);
    let window = MdEvent::Window {
        inst: inst(A),
        book: INTERACTIVE,
        lo: Ticks(95),
        hi: Ticks(105),
    };
    assert_eq!(books.apply(&window), Ok(Applied::Changed));
    let interactive = books.book(inst(A), INTERACTIVE).unwrap();
    assert_eq!(interactive.top(15).unwrap(), Top::default());
    let public = books.trading_book(inst(A)).unwrap();
    assert_eq!(public.window(), None);
    assert_eq!(public.top(15).unwrap().bids, vec![lvl(90, 1)]);

    // A gap on another feed, and a trade, leave both books as they were.
    let before = books.clone();
    for ev in [
        MdEvent::Health {
            inst: inst(A),
            feed: Feed::Trades,
            h: FeedHealth::Gap,
        },
        MdEvent::Health {
            inst: inst(A),
            feed: Feed::Book(PUBLIC),
            h: FeedHealth::Stale,
        },
        MdEvent::Trade {
            inst: inst(A),
            id: None,
            aggressor: fbc_core::Aggressor::Unknown,
            px: Ticks(100),
            qty: Lots::new(1).unwrap(),
        },
    ] {
        assert_eq!(books.apply(&ev), Ok(Applied::Unchanged));
    }
    assert_eq!(books, before);
}

#[test]
fn a_refused_event_is_counted_and_still_reaches_the_handler() {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let log = seen.clone();
    let mut keeper = BookKeeper::new(
        MdBooks::new(TradingBooks::new([]).unwrap()),
        move |env: Envelope<MdEvent>, books: &MdBooks| {
            log.borrow_mut().push((env.body, books.refused()))
        },
    );
    let stray_end = MdEvent::BookSnapshotEnd {
        inst: inst(A),
        book: PUBLIC,
    };
    let inverted = MdEvent::Window {
        inst: inst(A),
        book: INTERACTIVE,
        lo: Ticks(2),
        hi: Ticks(1),
    };
    let stamp = fbc_core::Stamp {
        ingest_seq: 0,
        kernel_rx: None,
        recv_mono: fbc_core::MonoNs(0),
        recv_wall: fbc_core::WallNs(0),
        conn: fbc_core::ConnKey { conn: 0, epoch: 0 },
    };
    for ev in [stray_end, inverted] {
        keeper.on_md(Envelope::new(stamp, fbc_core::VenueMeta::NONE, ev));
    }
    assert_eq!(*seen.borrow(), [(stray_end, 1), (inverted, 2)]);
    let books = keeper.books();
    assert_eq!(books.refused(), 2);
    // No instrument is configured to trade, so none has a trading book.
    assert_eq!(books.trading_book_id(inst(A)), None);
    assert!(books.trading_book(inst(A)).is_none());
    // The refused events still made the books they named, both awaiting a snapshot.
    for book in [PUBLIC, INTERACTIVE] {
        let b = books.book(inst(A), book).unwrap();
        assert_eq!(b.state(), BookState::AwaitingSnapshot);
    }
}

#[test]
fn an_instrument_configured_with_two_trading_books_is_refused() {
    let conflict =
        TradingBooks::new([(inst(A), PUBLIC), (inst(2), PUBLIC), (inst(A), INTERACTIVE)]);
    assert_eq!(
        conflict,
        Err(TradingBookConflict {
            inst: inst(A),
            first: PUBLIC,
            second: INTERACTIVE,
        })
    );
    assert!(
        conflict
            .unwrap_err()
            .to_string()
            .contains("two trading books")
    );
    // The same pair twice is one configuration, not a conflict.
    let same = TradingBooks::new([(inst(A), PUBLIC), (inst(A), PUBLIC)]).unwrap();
    assert_eq!(same.get(inst(A)), Some(PUBLIC));
    assert_eq!(same.get(inst(2)), None);
}
