//! FBC-zxv's done line: decoder replay of a journal the runtime wrote rebuilds the Binance USD-M
//! books byte-identically (BT-301, decision 0006). The Binance codec runs in fbc-runtime against
//! the conformance kit's stub server, which serves the committed hand-written fixtures
//! (FBC-z0l's `bookTicker` and partial-depth frames, FBC-tfb's clean diff-depth sequence and its
//! REST snapshot) with journaling on; fbc-book's books are built from the live events; the
//! journal is then replayed through fbc-runtime's decoder replay (FBC-3kz) into fresh books,
//! twice, and every book's canonical bytes equal the live book's. A hand-built journal then
//! takes the diff-depth book through a gap and its resync, which the stub's one fixed snapshot
//! response cannot: both replays rebuild the resynced book byte for byte.
//!
//! The session runs on real time with generous deadlines (a minute-long snapshot timeout, no
//! attempt deadline): the stub answers on the loopback at once, so nothing waits on them. A
//! paused clock would jump to a pending HTTP timeout while socket I/O is in flight.

mod common;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use common::{BTC, ETH, config, fixture, rest_fixture, specs};
use fbc_book::{BookState, Books};
use fbc_conformance::{Frame, HttpReply, HttpRoutes, Step, StubServer, WsScript};
use fbc_core::{
    BookId, BookSide, ConnKey, EndpointPlan, Envelope, Feed, HttpTag, InstrumentId, Lots, Lvl,
    MdEvent, MonoNs, Stamp, Subscription, Ticks, VenueConfig, VenueFactory, WallNs,
};
use fbc_journal::{
    ControlEvent, HttpResponseRec, JournalReader, JournalWriter, Opaque, Opcode, Record,
    RedactionKey, SinkConfig, journal_queue,
};
use fbc_runtime::{
    Connector, IngestClock, Journal, Liveness, MdReplay, MdReplayConfig, MdReplayCounters,
    MdSession, MdSessionConfig, ProxyConfig, RateLimiter, ReconnectPacing, SafetyReserve,
    WriteStall,
};
use fbc_venue_binance_usdm::{
    BOOK_DIFF, BOOK_PARTIAL, BinanceUsdm, KEY_REST_BASE_URL, KEY_SNAPSHOT_TIMEOUT, KEY_WS_BASE_URL,
    TOUCH_BOOK_TICKER,
};

/// The session's connection number, and the shard whose journal it writes.
const CONN: u16 = 2;
const SHARD: u16 = 1;

/// The diff-depth fixture's last event, e4: once it is applied every frame has been decoded.
const LAST_DIFF_U: u64 = 1_027_060;

/// Every book the session builds: BTC's and ETH's partial-depth books, and BTC's diff-depth book.
const BOOKS: [(InstrumentId, BookId); 3] =
    [(BTC, BOOK_PARTIAL), (ETH, BOOK_PARTIAL), (BTC, BOOK_DIFF)];

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn sub(inst: InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

/// Both instruments' touches and partial-depth books, and BTC's diff-depth book.
fn subs() -> BTreeSet<Subscription> {
    BTreeSet::from([
        sub(BTC, Feed::Touch(TOUCH_BOOK_TICKER)),
        sub(ETH, Feed::Touch(TOUCH_BOOK_TICKER)),
        sub(BTC, Feed::Book(BOOK_PARTIAL)),
        sub(ETH, Feed::Book(BOOK_PARTIAL)),
        sub(BTC, Feed::Book(BOOK_DIFF)),
    ])
}

/// The tests' configuration pointed at the stub: its WebSocket and HTTP origins, and a snapshot
/// timeout no loopback answer comes near.
fn stub_config(stub: &StubServer) -> VenueConfig {
    let mut cfg = config();
    cfg.insert(KEY_WS_BASE_URL, &stub.ws_url(""));
    cfg.insert(KEY_REST_BASE_URL, &stub.http_url(""));
    cfg.insert(KEY_SNAPSHOT_TIMEOUT, "60000ms");
    cfg
}

/// The one endpoint Binance plans for [`subs`] under `cfg`.
fn plan(cfg: &VenueConfig) -> EndpointPlan {
    let mut plans = BinanceUsdm.plan_md(cfg, &specs(), &subs()).unwrap();
    assert_eq!(plans.len(), 1);
    plans.remove(0)
}

/// The stub's script: the codec's one SUBSCRIBE is read and acknowledged, then every fixture
/// frame is pushed (touches, partial depth, then the clean diff-depth sequence); the
/// connection stays open.
fn script() -> WsScript {
    let push = |frame: String| Step::Push {
        conn: 0,
        frame: Frame::text(frame),
    };
    let ack = fixture("replies.jsonl").remove(0);
    let frames = [
        "book_ticker.jsonl",
        "partial_depth.jsonl",
        "diff_depth.jsonl",
    ]
    .into_iter()
    .flat_map(fixture);
    let mut steps = vec![Step::Accept, Step::Read { conn: 0 }, push(ack)];
    steps.extend(frames.map(push));
    WsScript::new(steps)
}

/// `GET /fapi/v1/depth` answers FBC-tfb's first anchor.
fn routes() -> HttpRoutes {
    HttpRoutes::from([(
        "/fapi/v1/depth".to_owned(),
        HttpReply {
            status: 200,
            body: rest_fixture("depth_snapshot.json").into_bytes(),
        },
    )])
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[7; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Books built from `events`, as a consumer builds them; every event is one fbc-book takes.
fn build(events: &[Envelope<MdEvent>]) -> Books {
    let mut books = Books::new();
    for env in events {
        books.apply(&env.body).unwrap();
    }
    books
}

/// Each of [`BOOKS`]' canonical bytes.
fn canonical(books: &Books) -> Vec<Vec<u8>> {
    BOOKS
        .iter()
        .map(|&(inst, book)| books.get(inst, book).unwrap().canonical_bytes())
        .collect()
}

/// Replays the journal at `root` through Binance's current decoders, as the session ran: the
/// events, the books built from them, and what the replay counted.
fn replay(root: &Path, cfg: &VenueConfig) -> (Vec<Envelope<MdEvent>>, Books, MdReplayCounters) {
    let seen = Seen::default();
    let keep = seen.clone();
    let config = MdReplayConfig {
        venue: &BinanceUsdm,
        cfg: cfg.clone(),
        plan: plan(cfg),
        specs: specs(),
        conn: CONN,
    };
    let mut replay = MdReplay::new(config, move |env| keep.borrow_mut().push(env)).unwrap();
    replay
        .run(JournalReader::open(root, SHARD).unwrap())
        .unwrap();
    let counters = replay.counters();
    drop(replay);
    let events = seen.take();
    let books = build(&events);
    (events, books, counters)
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn lvl(px: i64, qty: i64) -> Lvl {
    Lvl {
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

#[tokio::test]
async fn decoder_replay_of_the_journal_rebuilds_the_binance_books_byte_identically() {
    let stub = StubServer::start(script(), routes()).await.unwrap();
    let cfg = stub_config(&stub);
    let caps = BinanceUsdm.caps(&cfg).unwrap();
    let pacing = ReconnectPacing::new(ms(10), ms(100), 10, ms(60_000), Duration::MAX).unwrap();
    let session_config = MdSessionConfig {
        venue: &BinanceUsdm,
        cfg: cfg.clone(),
        plan: plan(&cfg),
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        http_max_body: 64 * 1024,
        conn: CONN,
        limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        liveness: Liveness::new(Duration::from_secs(3_600), ms(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };

    // Live: the session journals everything it takes in, and the handler keeps every event.
    let root = fresh_dir("binance_usdm_replay");
    let sink_config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(sink_config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let seen = Seen::default();
    let keep = seen.clone();
    let (mut session, control) =
        MdSession::new(session_config, move |env| keep.borrow_mut().push(env)).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(sink))));
    let watch = seen.clone();
    let script = async {
        stub.finished().await.unwrap();
        // The diff-depth book publishes once its snapshot is in, before or after the frames;
        // e4 applied means every pushed frame has been decoded.
        let applied = || {
            watch
                .borrow()
                .iter()
                .any(|e| e.venue_seq == Some(LAST_DIFF_U))
        };
        while !applied() {
            tokio::time::sleep(ms(2)).await;
        }
        drop(control);
    };
    // A regression that loses a frame, or one that keeps the session running once its control
    // drops, fails here instead of hanging: the bound covers the session and the script together
    // (Codex r4180053526).
    let both = async { tokio::join!(session.run(), script) };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(60), both)
        .await
        .expect("the session did not apply the fixtures and stop within 60 s");
    run.unwrap();
    writer.close().unwrap();
    let counters = session.counters();
    assert_eq!(counters.decode_errors, 0);

    // The session subscribed once and asked for the one snapshot the stub served.
    let conns = stub.connections();
    assert_eq!(conns.len(), 1);
    let [Frame::Text(subscribe)] = conns[0].received.as_slice() else {
        panic!("one SUBSCRIBE: {:?}", conns[0].received)
    };
    assert!(
        subscribe.starts_with(r#"{"id":1,"method":"SUBSCRIBE""#),
        "{subscribe}"
    );
    assert_eq!(
        stub.http_requests(),
        ["GET /fapi/v1/depth?symbol=BTCUSDT&limit=1000"]
    );

    // The live books: each partial-depth book is its one snapshot, and the diff-depth book is
    // FBC-tfb's snapshot with e1 to e4 applied (e0 is older than the snapshot).
    let live = seen.take();
    let live_books = build(&live);
    for (inst, book) in BOOKS {
        let state = live_books.get(inst, book).unwrap().state();
        assert_eq!(state, BookState::Valid { epoch: 1 }, "{inst:?} {book:?}");
    }
    let diff = live_books.get(BTC, BOOK_DIFF).unwrap().top(15).unwrap();
    assert_eq!(diff.bids, [lvl(740_545, 800), lvl(740_540, 4_000)]);
    assert_eq!(diff.asks, [lvl(740_560, 3_000), lvl(740_570, 500)]);
    let partial = live_books.get(ETH, BOOK_PARTIAL).unwrap().top(15).unwrap();
    assert_eq!(partial.bids, [lvl(18_195, 10_500), lvl(18_194, 2_000)]);
    assert_eq!(partial.asks, [lvl(18_196, 7_250)]);
    let touches = live
        .iter()
        .filter(|e| matches!(e.body, MdEvent::Touch { .. }));
    assert_eq!(touches.count(), 2);

    // Replayed twice from the journal: the same events, and books whose canonical bytes are
    // the live books', every time.
    let live_bytes = canonical(&live_books);
    let (first, first_books, first_counters) = replay(&root, &cfg);
    let (second, second_books, second_counters) = replay(&root, &cfg);
    assert_eq!(canonical(&first_books), live_bytes);
    assert_eq!(canonical(&second_books), live_bytes);
    assert_eq!(first, live);
    assert_eq!(second, live);
    assert_eq!(first_counters, second_counters);
    // One epoch: the acknowledgement and nine market-data frames, and the snapshot response.
    assert_eq!(
        first_counters,
        MdReplayCounters {
            epochs: 1,
            frames: 10,
            http: 1,
            ..MdReplayCounters::default()
        }
    );
    fs::remove_dir_all(&root).unwrap();
}

fn stamp(ingest_seq: u64) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: None,
        recv_mono: MonoNs(ingest_seq * 1_000),
        recv_wall: WallNs(1_700_000_000_000_000_000 + ingest_seq as i64),
        conn: ConnKey {
            conn: CONN,
            epoch: 0,
        },
    }
}

/// A hand-built journal of one epoch of BTC's diff-depth book through a gap: the session opens
/// and subscribes; e0 and e1 arrive and the first snapshot (tag 1) anchors the book; e2 arrives
/// and e4, whose `pu` names the missing e3, so the codec reports a gap and asks again (tag 2);
/// the resync snapshot is answered and e4 bridges it.
fn gap_journal(root: &Path) {
    let frames = fixture("diff_depth_gap.jsonl");
    let frame = |seq: u64, n: usize| Record::Inbound {
        stamp: stamp(seq),
        opcode: Opcode::Text,
        bytes: Opaque(frames[n].clone().into_bytes()),
        redact: Vec::new(),
    };
    let snapshot = |seq: u64, tag: u64, name: &str| Record::HttpResult {
        stamp: stamp(seq),
        tag: HttpTag(tag),
        result: Ok(HttpResponseRec {
            status: 200,
            headers: Vec::new(),
            body: Opaque(rest_fixture(name).into_bytes()),
            body_redact: Vec::new(),
        }),
    };
    let control = |ev: ControlEvent| Record::Control { at: MonoNs(0), ev };
    let conn = stamp(0).conn;
    let records = [
        control(ControlEvent::Opened(conn)),
        control(ControlEvent::Subscribe {
            conn,
            add: vec![sub(BTC, Feed::Book(BOOK_DIFF))],
            remove: Vec::new(),
        }),
        frame(1, 0),
        frame(2, 1),
        snapshot(3, 1, "depth_snapshot.json"),
        frame(4, 2),
        frame(5, 3),
        snapshot(6, 2, "depth_snapshot_resync.json"),
        control(ControlEvent::Closed(conn)),
    ];
    let mut writer = JournalWriter::create(root, SHARD, key()).unwrap();
    for record in &records {
        writer.append(stamp(0).recv_wall, record).unwrap();
    }
    writer.flush().unwrap();
}

#[test]
fn decoder_replay_of_a_hand_built_journal_rebuilds_the_resynced_book_every_time() {
    let root = fresh_dir("binance_usdm_replay_gap");
    gap_journal(&root);
    let cfg = config();
    let (first, first_books, first_counters) = replay(&root, &cfg);
    let (second, second_books, second_counters) = replay(&root, &cfg);
    assert_eq!(first, second);
    assert_eq!(first_counters, second_counters);
    assert_eq!(
        first_counters,
        MdReplayCounters {
            epochs: 1,
            frames: 4,
            http: 2,
            ..MdReplayCounters::default()
        }
    );
    let gaps = first
        .iter()
        .filter(|e| matches!(e.body, MdEvent::Health { .. }));
    assert_eq!(gaps.count(), 1);

    // The book is the resync snapshot under the second epoch with e4 applied: byte for byte a
    // book built by hand from those events, on both replays.
    let mut expected = Books::new();
    let level = |side, px, qty| MdEvent::Level {
        inst: BTC,
        book: BOOK_DIFF,
        side,
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    };
    for ev in [
        MdEvent::BookSnapshotBegin {
            inst: BTC,
            book: BOOK_DIFF,
            epoch: 2,
        },
        level(BookSide::Bid, 740_540, 4_000),
        level(BookSide::Ask, 740_560, 3_000),
        level(BookSide::Ask, 740_570, 500),
        MdEvent::BookSnapshotEnd {
            inst: BTC,
            book: BOOK_DIFF,
        },
        level(BookSide::Bid, 740_545, 800),
    ] {
        expected.apply(&ev).unwrap();
    }
    let bytes = |books: &Books| books.get(BTC, BOOK_DIFF).unwrap().canonical_bytes();
    assert_eq!(bytes(&first_books), bytes(&expected));
    assert_eq!(bytes(&second_books), bytes(&expected));
    let book = first_books.get(BTC, BOOK_DIFF).unwrap();
    assert_eq!(book.state(), BookState::Valid { epoch: 2 });
    fs::remove_dir_all(&root).unwrap();
}
