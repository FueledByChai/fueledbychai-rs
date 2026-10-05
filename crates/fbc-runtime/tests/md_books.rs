//! FBC-nij's done line: the runtime keeps one book per (instrument, book channel). Interleaved
//! snapshots and deltas of `Feed::Book(BookId(0))` and `Feed::Book(BookId(1))` of one
//! instrument, from one connection, build two separate books (a level or a gap on one leaves
//! the other unchanged), and the book exposed as the instrument's trading book is the configured
//! channel's, never the other one, even while the other is the only valid book.
//!
//! FBC-crh's done line: a connection's end invalidates the books it fed. Both book channels of
//! one instrument, valid on one session's connection, read invalid (and so does the trading
//! book) from the moment that connection drops until the new epoch's snapshot of each, while a
//! book another session's connection feeds stays valid throughout.

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use common::ScriptedWs;
use common::toy::{self, ToyVenue};
use fbc_book::{Applied, BookError, BookState, Top, Touch};
use fbc_core::{
    BookId, BookSide, ConnKey, EndpointPlan, Envelope, Feed, FeedHealth, InstrumentId, Lots, Lvl,
    MdEvent, MdTransport, Subscription, Ticks, VenueConfig, WireUrl,
};
use fbc_runtime::{
    BookHandler, BookKeeper, Connector, IngestClock, Liveness, MdBooks, MdHandler, MdSession,
    MdSessionConfig, ProxyConfig, ReconnectPacing, TradingBookConflict, TradingBooks, WriteStall,
};

const A: u32 = 1;
const B: u32 = 2;
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

/// A toy session on `server`'s connection `conn`, subscribed to `subs`.
fn session_config(
    venue: &'static ToyVenue,
    server: &ScriptedWs,
    subs: Vec<Subscription>,
    conn: u16,
    clock: IngestClock,
) -> MdSessionConfig {
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(server.url()),
            },
            subs,
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock,
        http_max_body: 1024,
        conn,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
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
    let config = session_config(
        venue,
        &server,
        vec![toy::book(A, PUBLIC), toy::book(A, INTERACTIVE)],
        3,
        IngestClock::new(),
    );
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

/// The connection epoch the hand-fed events of the unit tests come from.
fn from(conn: u16, epoch: u32) -> ConnKey {
    ConnKey { conn, epoch }
}

fn snapshot(books: &mut MdBooks, book: BookId, epoch: u32, levels: &[(BookSide, i64, i64)]) {
    snapshot_from(books, from(0, 0), A, book, epoch, levels);
}

fn snapshot_from(
    books: &mut MdBooks,
    conn: ConnKey,
    id: u32,
    book: BookId,
    epoch: u32,
    levels: &[(BookSide, i64, i64)],
) {
    let begin = MdEvent::BookSnapshotBegin {
        inst: inst(id),
        book,
        epoch,
    };
    assert_eq!(books.apply(conn, &begin), Ok(Applied::Changed));
    for &(side, px, qty) in levels {
        let level = MdEvent::Level {
            inst: inst(id),
            book,
            side,
            px: Ticks(px),
            qty: Lots::new(qty).unwrap(),
        };
        assert_eq!(books.apply(conn, &level), Ok(Applied::Changed));
    }
    let end = MdEvent::BookSnapshotEnd {
        inst: inst(id),
        book,
    };
    assert_eq!(books.apply(conn, &end), Ok(Applied::Changed));
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
    assert_eq!(books.apply(from(0, 0), &window), Ok(Applied::Changed));
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
        assert_eq!(books.apply(from(0, 0), &ev), Ok(Applied::Unchanged));
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

/// The states the handler saw: instrument A's two channels, A's trading book (the interactive
/// channel) and instrument B's public channel; `None` before a book's first event.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct States {
    a_public: Option<BookState>,
    a_interactive: Option<BookState>,
    trading: Option<BookState>,
    b: Option<BookState>,
}

impl States {
    fn of(books: &MdBooks) -> States {
        let state = |id, book| books.book(inst(id), book).map(|b| b.state());
        States {
            a_public: state(A, PUBLIC),
            a_interactive: state(A, INTERACTIVE),
            trading: books.trading_book(inst(A)).map(|b| b.state()),
            b: state(B, PUBLIC),
        }
    }
}

fn valid(state: Option<BookState>) -> bool {
    matches!(state, Some(BookState::Valid { .. }))
}

/// What reached the consumer's book handler: an event from a connection epoch, or an epoch's
/// end.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Saw {
    Event(ConnKey, MdEvent),
    End(ConnKey),
}

type Log = Rc<RefCell<Vec<(Saw, States)>>>;

/// The consumer's book handler: logs what it is told with the books' states then.
struct Recorder(Log);

impl BookHandler for Recorder {
    fn on_md(&mut self, env: Envelope<MdEvent>, books: &MdBooks) {
        let saw = Saw::Event(env.stamp.conn, env.body);
        self.0.borrow_mut().push((saw, States::of(books)));
    }

    fn on_epoch_end(&mut self, key: ConnKey, books: &MdBooks) {
        self.0.borrow_mut().push((Saw::End(key), States::of(books)));
    }
}

/// One keeper shared by two sessions, as a venue's sessions share its handler.
struct Shared(Rc<RefCell<BookKeeper<Recorder>>>);

impl MdHandler for Shared {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.0.borrow_mut().on_md(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().on_epoch_end(key);
    }
}

async fn until(log: &Log, done: impl Fn(&[(Saw, States)]) -> bool) {
    while !done(&log.borrow()) {
        tokio::time::sleep(ms(2)).await;
    }
}

fn last(log: &[(Saw, States)]) -> States {
    log.last().map_or(
        States {
            a_public: None,
            a_interactive: None,
            trading: None,
            b: None,
        },
        |(_, s)| *s,
    )
}

fn snapshot_end(saw: &Saw, id: u32, channel: BookId) -> bool {
    matches!(saw, Saw::Event(_, MdEvent::BookSnapshotEnd { inst: i, book }) if *i == inst(id) && *book == channel)
}

#[tokio::test]
async fn a_dropped_connection_invalidates_the_books_it_fed_until_each_ones_next_snapshot() {
    let mut first = ScriptedWs::start().await;
    let mut second = ScriptedWs::start().await;
    let venue = ToyVenue::leak();
    let clock = IngestClock::new();
    // Connection 3 feeds both channels of instrument A, connection 4 instrument B's public
    // channel; A trades on its interactive channel. One keeper takes both sessions' events.
    let a = session_config(
        venue,
        &first,
        vec![toy::book(A, PUBLIC), toy::book(A, INTERACTIVE)],
        3,
        clock.clone(),
    );
    let b = session_config(venue, &second, vec![toy::book(B, PUBLIC)], 4, clock);
    let log = Log::default();
    let keeper = BookKeeper::new(
        MdBooks::new(TradingBooks::new([(inst(A), INTERACTIVE)]).unwrap()),
        Recorder(log.clone()),
    );
    let keeper = Rc::new(RefCell::new(keeper));
    let (mut session_a, control_a) = MdSession::new(a, Shared(keeper.clone())).unwrap();
    let (mut session_b, control_b) = MdSession::new(b, Shared(keeper.clone())).unwrap();
    let watch = log.clone();
    let script = async move {
        let mut peer_a = first.accept().await;
        let mut peer_b = second.accept().await;
        assert!(peer_a.recv().await.ends_with("|plan=1,1"));
        assert_eq!(peer_a.recv().await, "sub|add=A,A");
        assert!(peer_b.recv().await.ends_with("|plan=2"));
        assert_eq!(peer_b.recv().await, "sub|add=B");
        for frame in [
            "begin|sym=A|book=0|epoch=1|seq=10",
            "lvl|sym=A|book=0|side=bid|px=100|qty=5|seq=11",
            "end|sym=A|book=0|seq=12",
            "begin|sym=A|book=1|epoch=1|seq=50",
            "lvl|sym=A|book=1|side=ask|px=101|qty=2|seq=51",
            "end|sym=A|book=1|seq=52",
        ] {
            peer_a.send(frame);
        }
        for frame in [
            "begin|sym=B|book=0|epoch=1|seq=1",
            "lvl|sym=B|book=0|side=bid|px=200|qty=3|seq=2",
            "end|sym=B|book=0|seq=3",
        ] {
            peer_b.send(frame);
        }
        until(&watch, |log| {
            let s = last(log);
            valid(s.a_public) && valid(s.a_interactive) && valid(s.b)
        })
        .await;

        // Connection 3 drops, and its session reconnects, its ended epoch's end told before.
        peer_a.drop_conn();
        let mut peer_a = first.accept().await;
        assert!(peer_a.recv().await.ends_with("|plan=1,1"));
        assert_eq!(peer_a.recv().await, "sub|add=A,A");
        // Connection 4 still feeds B meanwhile.
        peer_b.send("lvl|sym=B|book=0|side=bid|px=200|qty=8|seq=4");
        until(&watch, |log| {
            log.iter().any(|(saw, _)| {
                matches!(
                    saw,
                    Saw::Event(ConnKey { conn: 4, .. }, MdEvent::Level { qty, .. })
                        if *qty == Lots::new(8).unwrap()
                )
            })
        })
        .await;

        // The next epoch of connection 3 rebuilds the public channel first, then the trading
        // one.
        for frame in [
            "begin|sym=A|book=0|epoch=2|seq=20",
            "lvl|sym=A|book=0|side=bid|px=99|qty=1|seq=21",
            "end|sym=A|book=0|seq=22",
        ] {
            peer_a.send(frame);
        }
        until(&watch, |log| valid(last(log).a_public)).await;
        for frame in [
            "begin|sym=A|book=1|epoch=2|seq=60",
            "lvl|sym=A|book=1|side=ask|px=102|qty=4|seq=61",
            "end|sym=A|book=1|seq=62",
        ] {
            peer_a.send(frame);
        }
        until(&watch, |log| valid(last(log).trading)).await;
        drop((control_a, control_b));
        assert_eq!(peer_a.next().await, None);
        assert_eq!(peer_b.next().await, None);
    };
    let (ran_a, ran_b, ()) = tokio::join!(session_a.run(), session_b.run(), script);
    ran_a.unwrap();
    ran_b.unwrap();

    let log = log.borrow();
    let ended = ConnKey { conn: 3, epoch: 0 };
    let drop_at = log
        .iter()
        .position(|(saw, _)| matches!(saw, Saw::End(k) if k.conn == 3))
        .unwrap();
    assert_eq!(log[drop_at].0, Saw::End(ended));
    // Up to the drop, all three books were valid, and so was the trading book.
    let before = log[drop_at - 1].1;
    assert!(valid(before.a_public) && valid(before.a_interactive));
    assert!(valid(before.trading) && valid(before.b));
    // From the drop, both of A's books (and so its trading book) read as gapped; B's is valid.
    assert_eq!(
        log[drop_at].1,
        States {
            a_public: Some(BookState::Gapped),
            a_interactive: Some(BookState::Gapped),
            trading: Some(BookState::Gapped),
            b: before.b,
        }
    );
    // Each of A's books stays invalid until its own snapshot of the new epoch ends.
    let back = |channel| {
        drop_at
            + log[drop_at..]
                .iter()
                .position(|(saw, _)| snapshot_end(saw, A, channel))
                .unwrap()
    };
    let (public_back, interactive_back) = (back(PUBLIC), back(INTERACTIVE));
    assert!(public_back < interactive_back);
    for (saw, s) in &log[drop_at..public_back] {
        assert!(!valid(s.a_public), "{saw:?}");
    }
    for (saw, s) in &log[drop_at..interactive_back] {
        assert!(!valid(s.a_interactive) && !valid(s.trading), "{saw:?}");
    }
    let next = ConnKey { conn: 3, epoch: 1 };
    assert_eq!(
        log[public_back].1.a_public,
        Some(BookState::Valid { epoch: 2 })
    );
    assert_eq!(
        log[interactive_back].1,
        States {
            a_public: Some(BookState::Valid { epoch: 2 }),
            a_interactive: Some(BookState::Valid { epoch: 2 }),
            trading: Some(BookState::Valid { epoch: 2 }),
            b: before.b,
        }
    );
    for (saw, _) in &log[drop_at + 1..=interactive_back] {
        if let Saw::Event(conn, _) = saw
            && conn.conn == 3
        {
            assert_eq!(*conn, next, "{saw:?}");
        }
    }
    // B's book, fed by connection 4, was valid from its snapshot until its own session
    // stopped, through A's drop, and took the delta that came meanwhile.
    let b_valid = log
        .iter()
        .position(|(saw, _)| snapshot_end(saw, B, PUBLIC))
        .unwrap();
    let b_end = log
        .iter()
        .position(|(saw, _)| matches!(saw, Saw::End(k) if k.conn == 4))
        .unwrap();
    assert!(b_valid < drop_at && interactive_back < b_end);
    for (saw, s) in &log[b_valid..b_end] {
        assert_eq!(s.b, Some(BookState::Valid { epoch: 1 }), "{saw:?}");
    }
    assert!(log[drop_at..b_end].iter().any(|(saw, _)| matches!(
        saw,
        Saw::Event(ConnKey { conn: 4, .. }, MdEvent::Level { .. })
    )));
    // Stopping a session ends its epoch too: B's book is invalid once connection 4 has ended.
    assert_eq!(log[b_end].0, Saw::End(ConnKey { conn: 4, epoch: 0 }));
    assert_eq!(log[b_end].1.b, Some(BookState::Gapped));
    let books = keeper.borrow();
    let books = books.books();
    assert_eq!(books.refused(), 0);
    for (id, channel) in [(A, PUBLIC), (A, INTERACTIVE), (B, PUBLIC)] {
        assert_eq!(
            books.book(inst(id), channel).unwrap().state(),
            BookState::Gapped
        );
    }
}

#[test]
fn an_epochs_end_gaps_only_the_books_its_connection_fed_last_in_it_or_before() {
    let mut books = MdBooks::new(TradingBooks::new([(inst(A), PUBLIC)]).unwrap());
    let both = [(BookSide::Bid, 90, 1), (BookSide::Ask, 110, 1)];
    // A's public channel last fed by connection 3's epoch 0, its interactive one by epoch 1 of
    // the same connection; B's by connection 4; C's snapshot from connection 3's epoch 0 is still
    // in progress.
    snapshot_from(&mut books, from(3, 0), A, PUBLIC, 1, &both);
    snapshot_from(&mut books, from(3, 0), A, INTERACTIVE, 1, &both);
    snapshot_from(&mut books, from(3, 1), A, INTERACTIVE, 2, &both);
    snapshot_from(&mut books, from(4, 0), B, PUBLIC, 1, &both);
    let c_begin = MdEvent::BookSnapshotBegin {
        inst: inst(3),
        book: PUBLIC,
        epoch: 1,
    };
    assert_eq!(books.apply(from(3, 0), &c_begin), Ok(Applied::Changed));
    // A report that makes no book (a stale channel never fed) is no book to invalidate.
    let stale = MdEvent::Health {
        inst: inst(3),
        feed: Feed::Book(INTERACTIVE),
        h: FeedHealth::Stale,
    };
    assert_eq!(books.apply(from(3, 0), &stale), Ok(Applied::Unchanged));

    // An epoch no book came from changes nothing.
    let before = books.clone();
    assert_eq!(books.end_epoch(from(5, 0)), 0);
    assert_eq!(books, before);

    assert_eq!(books.end_epoch(from(3, 0)), 2);
    let state = |books: &MdBooks, id, book| books.book(inst(id), book).unwrap().state();
    assert_eq!(state(&books, A, PUBLIC), BookState::Gapped);
    assert_eq!(state(&books, A, INTERACTIVE), BookState::Valid { epoch: 2 });
    assert_eq!(state(&books, B, PUBLIC), BookState::Valid { epoch: 1 });
    let c = books.book(inst(3), PUBLIC).unwrap();
    assert_eq!(c.state(), BookState::Gapped);
    assert_eq!(c.snapshot_in_progress(), None);
    assert!(books.book(inst(3), INTERACTIVE).is_none());
    assert_eq!(
        books.trading_book(inst(A)).unwrap().touch(),
        Err(BookError::NotValid(BookState::Gapped))
    );

    // A later epoch's end covers a book last fed in an earlier epoch of its connection, whose end
    // was never told; and a delta of a book already gapped leaves it gapped.
    snapshot_from(&mut books, from(3, 2), A, PUBLIC, 3, &both);
    let delta = MdEvent::Level {
        inst: inst(3),
        book: PUBLIC,
        side: BookSide::Bid,
        px: Ticks(95),
        qty: Lots::new(1).unwrap(),
    };
    assert_eq!(
        books.apply(from(3, 0), &delta),
        Ok(Applied::DroppedNotValid)
    );
    assert_eq!(books.end_epoch(from(3, 1)), 2);
    assert_eq!(state(&books, A, INTERACTIVE), BookState::Gapped);
    assert_eq!(state(&books, A, PUBLIC), BookState::Valid { epoch: 3 });
    assert_eq!(state(&books, B, PUBLIC), BookState::Valid { epoch: 1 });
    assert_eq!(books.refused(), 0);
}
