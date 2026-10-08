//! FBC-8qd's done line: decoder replay of a journal the runtime wrote rebuilds the Paradex books
//! byte-identically (BT-401, decision 0006). The Paradex codec runs in fbc-runtime against the
//! conformance kit's stub server, which serves the committed hand-built SBE fixtures of two
//! markets (BTC-USD-PERP's and ETH-USD-PERP's `deltas` books, BTC's bbo and a trade) with
//! journaling on. BTC's book breaks its sequence on the first connection, so the codec asks for
//! the reconnect decision 0022 decides and both books are rebuilt from fresh snapshots on the
//! second. fbc-book's books are built from the live events; the journal is then replayed
//! through fbc-runtime's decoder replay (FBC-3kz), a fresh codec per epoch, into fresh books,
//! twice, and both books' canonical bytes equal the live books'. A hand-built journal then takes
//! BTC's book through a backwards seq_no and its resync: both replays rebuild the resynced book
//! byte for byte.
//!
//! The session runs on real time: Paradex asks for no HTTP and the stub answers on the loopback
//! at once, so nothing waits on a deadline; a 60 s bound fails a hang instead.

mod md;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use fbc_book::{BookState, Books};
use fbc_conformance::{Frame, HttpRoutes, Step, StubServer, WsScript};
use fbc_core::{
    ConnKey, Effects, EndpointPlan, Envelope, Feed, FeedHealth, InstrumentId, Lots, Lvl, MdCodec,
    MdEvent, MonoNs, RawFrame, Stamp, Subscription, Ticks, VenueConfig, VenueFactory, WallNs,
};
use fbc_journal::{
    ControlEvent, JournalReader, JournalWriter, Opaque, Opcode, Record, RedactionKey, SinkConfig,
    journal_queue,
};
use fbc_runtime::{
    Connector, IngestClock, Journal, Liveness, MdReplay, MdReplayConfig, MdReplayCounters,
    MdSession, MdSessionConfig, ProxyConfig, RateLimiter, ReconnectPacing, SafetyReserve,
    WriteStall,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::{MD_STREAM, MD_URL};
use fbc_venue_paradex::md::{BBO, DELTAS, ParadexMd};
use md::{BTC, ETH, decode_with, frame, specs};

/// The session's connection number, and the shard whose journal it writes.
const CONN: u16 = 4;
const SHARD: u16 = 2;

/// ETH's delta at seq 503, the second connection's last frame that pushes an event: once it is
/// applied every frame has been decoded.
const LAST_SEQ: u64 = 503;

/// Both markets' `deltas` books.
const BOOKS: [InstrumentId; 2] = [BTC, ETH];

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn sub(inst: InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

/// Both markets' `deltas` books, and BTC's bbo and trades: one connection carries them all.
fn subs() -> BTreeSet<Subscription> {
    BTreeSet::from([
        sub(BTC, Feed::Touch(BBO)),
        sub(BTC, Feed::Trades),
        sub(BTC, Feed::Book(DELTAS)),
        sub(ETH, Feed::Book(DELTAS)),
    ])
}

/// A configuration whose market-data URL is `url`.
fn config(url: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, url);
    cfg
}

/// The one endpoint Paradex plans for [`subs`] under `cfg`.
fn plan(cfg: &VenueConfig) -> EndpointPlan {
    let mut plans = ParadexFactory.plan_md(cfg, &specs(), &subs()).unwrap();
    assert_eq!(plans.len(), 1);
    plans.remove(0)
}

/// The stub's script, two connections. On each, the codec's four subscribe frames are read and
/// acknowledged. The first then carries BTC's snapshot at seq 1000 and deltas 1001 and 1002,
/// ETH's snapshot at 500 and delta 501, BTC's bbo and a trade, interleaved, and last BTC's
/// delta at 1004, which skips 1003: the codec reports the gap and asks for a reconnect. The
/// second carries the fresh snapshots a new subscription receives, BTC's at 2000 and ETH's at
/// 502, and deltas from them; it stays open.
fn script() -> WsScript {
    let push = |conn: usize, name: &str| Step::Push {
        conn,
        frame: Frame::Binary(frame(name)),
    };
    let mut steps = Vec::new();
    for (conn, frames) in [
        (
            0,
            &[
                "book-snapshot.sbe.txt",
                "eth-book-snapshot-500.sbe.txt",
                "book-delta-1001.sbe.txt",
                "bbo.sbe.txt",
                "eth-book-delta-501.sbe.txt",
                "book-delta-1002.sbe.txt",
                "trade.sbe.txt",
                "book-delta-1004.sbe.txt",
            ][..],
        ),
        (
            1,
            &[
                "book15-snapshot-2000.sbe.txt",
                "eth-book-snapshot-502.sbe.txt",
                "book15-delta-2001.sbe.txt",
                "book15-delta-2002.sbe.txt",
                "book15-delta-2003-empty.sbe.txt",
                "eth-book-delta-503.sbe.txt",
            ][..],
        ),
    ] {
        steps.push(Step::Accept);
        steps.extend((0..subs().len()).map(|_| Step::Read { conn }));
        steps.extend((1..=subs().len()).map(|id| Step::Push {
            conn,
            frame: Frame::text(format!(r#"{{"jsonrpc":"2.0","result":{{}},"id":{id}}}"#)),
        }));
        steps.extend(frames.iter().map(|name| push(conn, name)));
    }
    WsScript::new(steps)
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[9; 32]).unwrap())
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
        .map(|&inst| books.get(inst, DELTAS).unwrap().canonical_bytes())
        .collect()
}

/// Replays the journal at `root` through Paradex's current decoders, as the session ran: the
/// events, the books built from them, and what the replay counted.
fn replay(root: &Path, cfg: &VenueConfig) -> (Vec<Envelope<MdEvent>>, Books, MdReplayCounters) {
    let seen = Seen::default();
    let keep = seen.clone();
    let config = MdReplayConfig {
        venue: &ParadexFactory,
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

/// The book gaps among `events`, by market.
fn gaps(events: &[Envelope<MdEvent>]) -> Vec<InstrumentId> {
    let gap = |e: &Envelope<MdEvent>| match e.body {
        MdEvent::Health {
            inst,
            feed: Feed::Book(DELTAS),
            h: FeedHealth::Gap,
        } => Some(inst),
        _ => None,
    };
    events.iter().filter_map(gap).collect()
}

#[tokio::test]
async fn decoder_replay_of_the_journal_rebuilds_the_paradex_books_byte_identically() {
    let stub = StubServer::start(script(), HttpRoutes::new())
        .await
        .unwrap();
    let cfg = config(&stub.ws_url("/v1"));
    let caps = ParadexFactory.caps(&cfg).unwrap();
    let pacing = ReconnectPacing::new(ms(10), ms(100), 10, ms(60_000), Duration::MAX).unwrap();
    let session_config = MdSessionConfig {
        venue: &ParadexFactory,
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
    let root = fresh_dir("paradex_replay");
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
        // ETH's delta at 503 applied means every pushed frame has been decoded.
        let applied = || watch.borrow().iter().any(|e| e.venue_seq == Some(LAST_SEQ));
        while !applied() {
            tokio::time::sleep(ms(2)).await;
        }
        drop(control);
    };
    // A regression that loses a frame or never reconnects, or one that keeps the session
    // running once its control drops, fails here instead of hanging.
    let both = async { tokio::join!(session.run(), script) };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(60), both)
        .await
        .expect("the session did not apply the fixtures and stop within 60 s");
    run.unwrap();
    writer.close().unwrap();
    assert_eq!(session.counters().decode_errors, 0);

    // Two connections, each subscribed once to the four channels, in the subscriptions' order: the gap's resync was the
    // reconnect, with no subscribe frame of the codec's own.
    let conns = stub.connections();
    assert_eq!(conns.len(), 2);
    for conn in &conns {
        let channels: Vec<String> = conn
            .received
            .iter()
            .map(|f| {
                let Frame::Text(text) = f else {
                    panic!("a text subscribe: {f:?}")
                };
                let v: serde_json::Value = serde_json::from_str(text).unwrap();
                assert_eq!(v["method"], "subscribe", "{text}");
                v["params"]["channel"].as_str().unwrap().to_owned()
            })
            .collect();
        assert_eq!(
            channels,
            [
                "bbo.BTC-USD-PERP",
                "order_book.BTC-USD-PERP.deltas",
                "trades.BTC-USD-PERP",
                "order_book.ETH-USD-PERP.deltas",
            ]
        );
    }

    // The live books: BTC's gapped on the first connection and is the second's snapshot at
    // 2000 with deltas to 2002 (the REST snapshot ../rest/orderbook-btc-2002.json's book);
    // ETH's is its second snapshot, at 502, with 503 applied.
    let live = seen.take();
    assert_eq!(gaps(&live), [BTC]);
    let live_books = build(&live);
    for inst in BOOKS {
        let state = live_books.get(inst, DELTAS).unwrap().state();
        assert_eq!(state, BookState::Valid { epoch: 1 }, "{inst:?}");
    }
    let btc = live_books.get(BTC, DELTAS).unwrap().top(15).unwrap();
    assert_eq!((btc.bids.len(), btc.asks.len()), (15, 15));
    assert_eq!(btc.bids[..2], [lvl(620_002, 333), lvl(620_000, 100)]);
    assert_eq!(btc.asks[..2], [lvl(620_005, 100), lvl(620_015, 300)]);
    let eth = live_books.get(ETH, DELTAS).unwrap().top(15).unwrap();
    assert_eq!(
        eth.bids,
        [lvl(24_010, 1_000), lvl(24_005, 2_500), lvl(24_004, 5_000)]
    );
    assert_eq!(eth.asks, [lvl(24_013, 500)]);
    let others = |kind: fn(&MdEvent) -> bool| live.iter().filter(|e| kind(&e.body)).count();
    assert_eq!(others(|e| matches!(e, MdEvent::Touch { .. })), 1);
    assert_eq!(others(|e| matches!(e, MdEvent::Trade { .. })), 1);

    // Replayed twice from the journal, a fresh codec per epoch: the same events, and books
    // whose canonical bytes are the live books', every time.
    let live_bytes = canonical(&live_books);
    let (first, first_books, first_counters) = replay(&root, &cfg);
    let (second, second_books, second_counters) = replay(&root, &cfg);
    assert_eq!(canonical(&first_books), live_bytes);
    assert_eq!(canonical(&second_books), live_bytes);
    assert_eq!(first, live);
    assert_eq!(second, live);
    assert_eq!(first_counters, second_counters);
    // Two epochs: on each, four acknowledgements and its market-data frames.
    assert_eq!(
        first_counters,
        MdReplayCounters {
            epochs: 2,
            frames: 4 + 8 + 4 + 6,
            ..MdReplayCounters::default()
        }
    );
    fs::remove_dir_all(&root).unwrap();
}

fn stamp(epoch: u32, ingest_seq: u64) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: None,
        recv_mono: MonoNs(ingest_seq * 1_000),
        recv_wall: WallNs(1_700_000_000_000_000_000 + ingest_seq as i64),
        conn: ConnKey { conn: CONN, epoch },
    }
}

/// A hand-built journal of BTC's `deltas` book over two epochs. On the first, the snapshot at
/// seq 1000 and deltas 1001 and 1002 arrive, then 1001 again, a backwards seq_no: the codec
/// reports the gap and asks for a reconnect, which replay does not execute. The second epoch
/// subscribes again and its snapshot at 2000 and delta 2001 rebuild the book.
fn gap_journal(root: &Path) {
    let mut seq = 0;
    let mut frame_rec = |epoch: u32, name: &str| {
        seq += 1;
        Record::Inbound {
            stamp: stamp(epoch, seq),
            opcode: Opcode::Binary,
            bytes: Opaque(frame(name)),
            redact: Vec::new(),
        }
    };
    let control = |ev: ControlEvent| Record::Control { at: MonoNs(0), ev };
    let (first, second) = (stamp(0, 0).conn, stamp(1, 0).conn);
    let subscribe = |conn| {
        control(ControlEvent::Subscribe {
            conn,
            add: vec![sub(BTC, Feed::Book(DELTAS))],
            remove: Vec::new(),
        })
    };
    let records = [
        control(ControlEvent::Opened(first)),
        subscribe(first),
        frame_rec(0, "book-snapshot.sbe.txt"),
        frame_rec(0, "book-delta-1001.sbe.txt"),
        frame_rec(0, "book-delta-1002.sbe.txt"),
        frame_rec(0, "book-delta-1001.sbe.txt"),
        control(ControlEvent::Closed(first)),
        control(ControlEvent::Opened(second)),
        subscribe(second),
        frame_rec(1, "book15-snapshot-2000.sbe.txt"),
        frame_rec(1, "book15-delta-2001.sbe.txt"),
        control(ControlEvent::Closed(second)),
    ];
    let mut writer = JournalWriter::create(root, SHARD, key()).unwrap();
    for record in &records {
        writer.append(stamp(0, 0).recv_wall, record).unwrap();
    }
    writer.flush().unwrap();
}

#[test]
fn decoder_replay_of_a_hand_built_journal_rebuilds_the_resynced_book_every_time() {
    let root = fresh_dir("paradex_replay_gap");
    gap_journal(&root);
    let cfg = config("wss://stub.invalid/v1");
    let (first, first_books, first_counters) = replay(&root, &cfg);
    let (second, second_books, second_counters) = replay(&root, &cfg);
    assert_eq!(first, second);
    assert_eq!(first_counters, second_counters);
    assert_eq!(
        first_counters,
        MdReplayCounters {
            epochs: 2,
            frames: 6,
            ..MdReplayCounters::default()
        }
    );
    assert_eq!(gaps(&first), [BTC]);

    // The book is the second epoch's snapshot at 2000 with 2001 applied: byte for byte a book
    // built by hand from the same frames through a fresh codec, on both replays.
    let mut codec = ParadexMd::new(MD_STREAM);
    let book = [sub(BTC, Feed::Book(DELTAS))];
    codec
        .subscribe(&book, &[], &specs(), &mut Effects::new())
        .unwrap();
    let mut expected = Books::new();
    for name in ["book15-snapshot-2000.sbe.txt", "book15-delta-2001.sbe.txt"] {
        let out = decode_with(&mut codec, RawFrame::Binary(&frame(name)));
        out.result.unwrap();
        for (_, ev) in &out.events {
            expected.apply(ev).unwrap();
        }
    }
    let bytes = |books: &Books| books.get(BTC, DELTAS).unwrap().canonical_bytes();
    assert_eq!(bytes(&first_books), bytes(&expected));
    assert_eq!(bytes(&second_books), bytes(&expected));
    let book = first_books.get(BTC, DELTAS).unwrap();
    assert_eq!(book.state(), BookState::Valid { epoch: 1 });
    let top = book.top(1).unwrap();
    assert_eq!(top.bids.len() + top.asks.len(), 2);
    fs::remove_dir_all(&root).unwrap();
}
