//! FBC-u4so's done line: md_watch's session wiring, run against the conformance kit's stub
//! server on 127.0.0.1, prints the touch, book, trade and reconnect lines both venues' frames
//! call for, and its `--help` documents every argument.
//!
//! FBC-9o7e's done line, at the end of this file: `--hibachi <symbol>` reads the contract from a
//! stub's `GET /market/exchange-info` (`fixtures/hibachi/rest/`) and prints the hand-built
//! `/ws/market` frames of `fixtures/hibachi/md/` as Hibachi's `ask_bid_price` touch, its
//! orderbook's touch and top of book, each line stamped with its receive time; a book whose
//! frames come back at another granularity or depth gaps on every Snapshot with
//! `param_mismatches` counted, the sign the owner's first live run reads; the usage text and the
//! crate's README show the command; and nothing in the repository runs md_watch against a venue.
//! Beside it (PR #213's reviews): the README's other outcomes as the stub plays them, a book
//! whose Snapshots take the fields but are thin (`thin_sides`) and an orderbook subscription
//! left unanswered (`resubscribes`, `reconnects`, `RECONNECT`), each against the README's own
//! signature for it; and `--socks5` carrying the contract read as it carries the WebSocket.
//!
//! Two stubs stand in for the venues, serving the committed hand-built fixtures: Paradex's SBE
//! frames (`fixtures/paradex/md/`: BTC-USD-PERP's `deltas` book, its bbo and a trade) and
//! Binance's frames and REST snapshot (`fixtures/binance-usdm/`: BTCUSDT's `bookTicker` and its
//! clean diff-depth sequence). The Paradex stub closes the first connection after its frames,
//! the scripted drop: md_watch prints the reconnect, and the books it rebuilds on the second
//! connection from its fresh snapshot. md_watch runs with its default grids, as the owner runs
//! it; the session runs on real time, bounded at 60 s so a regression fails instead of hanging.

#[path = "../examples/md_watch/args.rs"]
mod args;
#[path = "../examples/md_watch/hibachi.rs"]
mod hibachi;
#[path = "../examples/md_watch/watch.rs"]
mod watch;

use std::cell::RefCell;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use args::{HibachiOpts, Market, Options, ParadexTouch, Parsed, USAGE};
use fbc_conformance::{Frame, HttpReply, HttpRoutes, Responder, Step, StubServer, WsScript};
use fbc_core::WallNs;
use fbc_runtime::ProxyConfig;
use rust_decimal::Decimal;

fn fixture_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

/// A hand-built Paradex frame: whitespace-separated hex bytes, `#` to the end of a line a
/// comment.
fn sbe(name: &str) -> Frame {
    let path = fixture_path(&format!("paradex/md/{name}"));
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let bytes = text
        .lines()
        .map(|line| line.split('#').next().unwrap())
        .flat_map(str::split_whitespace)
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect();
    Frame::Binary(bytes)
}

/// The lines of a Binance fixture, each a frame.
fn jsonl(name: &str) -> Vec<String> {
    let path = fixture_path(&format!("binance-usdm/md/{name}"));
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_owned)
        .collect()
}

/// Paradex: on each connection md_watch's three subscribes (bbo, deltas book, trades) are read
/// and acknowledged. The first then carries BTC's snapshot at seq 1000, deltas 1001 and 1002
/// with the bbo between them, and a trade, and is closed: the scripted drop. The second carries
/// the fresh snapshot a new subscription receives, at 2000, and the delta 2001; it stays open.
fn paradex_script() -> WsScript {
    let mut steps = Vec::new();
    for (conn, frames) in [
        (
            0,
            &[
                "book-snapshot.sbe.txt",
                "book-delta-1001.sbe.txt",
                "bbo.sbe.txt",
                "book-delta-1002.sbe.txt",
                "trade.sbe.txt",
            ][..],
        ),
        (
            1,
            &["book15-snapshot-2000.sbe.txt", "book15-delta-2001.sbe.txt"][..],
        ),
    ] {
        steps.push(Step::Accept);
        steps.extend((0..3).map(|_| Step::Read { conn }));
        steps.extend((1..=3).map(|id| Step::Push {
            conn,
            frame: Frame::text(format!(r#"{{"jsonrpc":"2.0","result":{{}},"id":{id}}}"#)),
        }));
        steps.extend(frames.iter().map(|name| Step::Push {
            conn,
            frame: sbe(name),
        }));
        if conn == 0 {
            steps.push(Step::Close { conn });
        }
    }
    WsScript::new(steps)
}

/// Binance: md_watch's one SUBSCRIBE is read and acknowledged, then BTCUSDT's `bookTicker` and
/// the clean diff-depth sequence are pushed; `GET /fapi/v1/depth` answers its anchor.
fn binance_stub() -> (WsScript, HttpRoutes) {
    let push = |frame: String| Step::Push {
        conn: 0,
        frame: Frame::text(frame),
    };
    let ack = jsonl("replies.jsonl").remove(0);
    let ticker = jsonl("book_ticker.jsonl").remove(0);
    let mut steps = vec![
        Step::Accept,
        Step::Read { conn: 0 },
        push(ack),
        push(ticker),
    ];
    steps.extend(jsonl("diff_depth.jsonl").into_iter().map(push));
    let snapshot = fixture_path("binance-usdm/rest/depth_snapshot.json");
    let body = fs::read_to_string(snapshot).unwrap().trim_end().to_owned();
    let routes = HttpRoutes::from([(
        "/fapi/v1/depth".to_owned(),
        HttpReply {
            status: 200,
            body: body.into_bytes(),
        },
    )]);
    (WsScript::new(steps), routes)
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| (*a).to_owned()).collect()
}

/// The lines whose venue (their second word) is `venue`, each with the indented level rows
/// under it, in order.
fn of(lines: &[String], venue: &str) -> Vec<String> {
    let mut keep = false;
    let mut kept = Vec::new();
    for line in lines {
        if !line.starts_with("  ") {
            keep = line.split_whitespace().nth(1) == Some(venue);
        }
        if keep {
            kept.push(line.clone());
        }
    }
    kept
}

#[tokio::test]
async fn md_watch_prints_both_venues_touches_and_books_and_a_reconnect_after_a_drop() {
    let paradex = StubServer::start(paradex_script(), HttpRoutes::new())
        .await
        .unwrap();
    let (script, routes) = binance_stub();
    let binance = StubServer::start(script, routes).await.unwrap();
    let parsed = args::parse(strings(&[
        "--paradex",
        "BTC-USD-PERP",
        "--binance",
        "BTCUSDT",
        "--top",
        "2",
        "--paradex-url",
        &paradex.ws_url("/v1"),
        "--binance-ws",
        &binance.ws_url(""),
        "--binance-rest",
        &binance.http_url(""),
    ]))
    .unwrap();
    let Parsed::Watch(opts) = parsed else {
        panic!("not a watch: {parsed:?}")
    };

    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: watch::Out = buf.clone();
    let text = || String::from_utf8(buf.borrow().clone()).unwrap();
    // Once both scripts are played and the last frame of each venue is printed, md_watch is
    // stopped: Paradex's book after the delta 2001 on the second connection, and Binance's
    // diff-depth book after its last delta.
    let stop = async {
        paradex.finished().await.unwrap();
        binance.finished().await.unwrap();
        let done = || {
            let t = text();
            t.contains("61999.5 x 0.75 | ask 62001.5 x 0.3")
                && t.contains("2 bid 7405.4 x 4 | ask 7405.7 x 0.5")
        };
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let run = watch::run(&opts, out, false, stop);
    let Ok(ran) = tokio::time::timeout(Duration::from_secs(60), run).await else {
        panic!(
            "md_watch did not print both venues' frames and stop within 60 s; it printed:\n{}",
            text()
        )
    };
    ran.unwrap();
    let printed = text();
    let lines: Vec<String> = printed.lines().map(str::to_owned).collect();

    assert_eq!(
        lines[0],
        "md_watch: paradex BTC-USD-PERP (bbo, trades, deltas book), binance BTCUSDT \
         (bookTicker, diff-depth book); market data only, no order is ever sent"
    );
    // Paradex, one connection's frames in order: the snapshot's book; 1001 removes the bid at
    // 62000.4 and leaves 1.25 at the ask; the bbo; 1002 adds a better bid; the trade. Then the
    // drop, and the second connection's fresh snapshot at 2000 and its delta 2001, which
    // removes the ask at 62001; md_watch's stop closes it.
    let book = "BOOK paradex BTC-USD-PERP deltas";
    let spread_8 = "mid 62000.75 spread 0.08bps";
    let second = "bid 62000 x 0.1 ask 62000.5 x 0.1 mid 62000.25 spread 0.08bps";
    assert_eq!(
        of(&lines[1..], "paradex"),
        [
            format!("{book} bid 62000.5 x 0.25 ask 62001 x 1.5 {spread_8}"),
            "  1 bid 62000.5 x 0.25 | ask 62001 x 1.5".to_owned(),
            "  2 bid 62000.4 x 1 | ask 62001.5 x 2".to_owned(),
            format!("{book} bid 62000.5 x 0.25 ask 62001 x 1.25 {spread_8}"),
            "  1 bid 62000.5 x 0.25 | ask 62001 x 1.25".to_owned(),
            "  2 bid - | ask 62001.5 x 2".to_owned(),
            format!("TOUCH paradex BTC-USD-PERP bbo bid 62000.5 x 0.25 ask 62001 x 1.5 {spread_8}"),
            format!("{book} bid 62000.6 x 0.5 ask 62001 x 1.25 mid 62000.8 spread 0.06bps"),
            "  1 bid 62000.6 x 0.5 | ask 62001 x 1.25".to_owned(),
            "  2 bid 62000.5 x 0.25 | ask 62001.5 x 2".to_owned(),
            "TRADE paradex BTC-USD-PERP sell 0.125 @ 62000".to_owned(),
            "RECONNECT paradex conn 0 epoch 0 ended".to_owned(),
            format!("{book} {second}"),
            "  1 bid 62000 x 0.1 | ask 62000.5 x 0.1".to_owned(),
            "  2 bid 61999.5 x 0.2 | ask 62001 x 0.2".to_owned(),
            format!("{book} {second}"),
            "  1 bid 62000 x 0.1 | ask 62000.5 x 0.1".to_owned(),
            "  2 bid 61999.5 x 0.75 | ask 62001.5 x 0.3".to_owned(),
            "CLOSED paradex conn 0 epoch 1 ended".to_owned(),
        ]
    );

    // Binance: the touch, then the diff-depth book, which publishes once its REST snapshot is
    // in and so may first show the snapshot with some of the deltas the stub pushed meanwhile;
    // whenever it publishes, its last line is the book after every delta: the snapshot's
    // 7405.30 bid removed, 7405.45 added, the 7405.50 ask removed and 7405.70 added. Nothing
    // dropped, so nothing reconnected and no book gapped.
    let binance_lines = of(&lines[1..], "binance");
    assert_eq!(
        binance_lines[0],
        "TOUCH binance BTCUSDT bookTicker bid 25351.9 x 31.21 ask 25365.2 x 40.66 \
         mid 25358.55 spread 5.24bps"
    );
    let n = binance_lines.len();
    assert_eq!(
        binance_lines[n - 4..],
        [
            "BOOK binance BTCUSDT diff-depth bid 7405.45 x 0.8 ask 7405.6 x 3 mid 7405.525 \
             spread 0.20bps",
            "  1 bid 7405.45 x 0.8 | ask 7405.6 x 3",
            "  2 bid 7405.4 x 4 | ask 7405.7 x 0.5",
            "CLOSED binance conn 64 epoch 0 ended",
        ]
    );
    for line in &binance_lines[1..n - 1] {
        let kind = line.split_whitespace().next().unwrap();
        assert!(kind == "BOOK" || line.starts_with("  "), "{line}");
    }
    assert_eq!(binance.connections().len(), 1);
    assert_eq!(paradex.connections().len(), 2);
    // Every line is the header or one venue's.
    let all = of(&lines[1..], "paradex").len() + n;
    assert_eq!(lines.len(), 1 + all);
}

#[test]
fn help_documents_every_argument() {
    assert_eq!(args::parse(strings(&["--help"])), Ok(Parsed::Help));
    assert_eq!(
        args::parse(strings(&["--paradex", "X", "-h"])),
        Ok(Parsed::Help)
    );
    for flag in [
        "--paradex <MARKET>",
        "--paradex-touch <WHICH>",
        "--binance <SYMBOL>",
        "--socks5 <HOST:PORT>",
        "--seconds <N>",
        "--top <N>",
        "--paradex-url <URL>",
        "--binance-ws <URL>",
        "--binance-rest <URL>",
        "--paradex-tick <DEC>",
        "--paradex-step <DEC>",
        "--binance-tick <DEC>",
        "--binance-step <DEC>",
        "--hibachi <SYMBOL>",
        "--hibachi-rest <URL>",
        "--hibachi-ws <URL>",
        "--hibachi-timeout <MS>",
        "--hibachi-depth <N>",
        "--hibachi-granularity <DEC>",
        "--hibachi-retry <MS>",
        "--help",
    ] {
        assert!(USAGE.contains(flag), "--help does not document {flag}");
    }
    // Every kind of line md_watch prints, the --top level rows and the stop's CLOSED included.
    for line in [
        "TOUCH <venue>",
        "BOOK <venue>",
        "<rank> bid <px> x <size> | ask <px> x <size>",
        "TRADE <venue>",
        "HEALTH <venue>",
        "MARKET <venue> <market> <open|closed ...>",
        "COUNTS <venue> <market> <counter> <n> ...",
        "STATUS <venue> <market> <trading|post-only|reduce-only|cancel-only|halted|delisted>",
        "RECONNECT <venue> conn <n> epoch <e> ended",
        "CLOSED <venue> conn <n> epoch <e> ended",
    ] {
        assert!(USAGE.contains(line), "--help does not document {line}");
    }
    assert!(USAGE.contains("never sends an order and never reads a credential"));
}

#[test]
fn the_command_line_is_parsed_with_defaults_and_refused_with_a_reason() {
    let finest: Decimal = "0.00000001".parse().unwrap();
    let parsed = args::parse(strings(&["--paradex", "ETH-USD-PERP", "--seconds", "30"]));
    let want = Options {
        paradex: Some(Market {
            symbol: "ETH-USD-PERP".to_owned(),
            tick: finest,
            step: finest,
        }),
        paradex_touch: ParadexTouch::Bbo,
        binance: None,
        hibachi: None,
        proxy: ProxyConfig::Direct,
        seconds: Some(30),
        top: 0,
        paradex_url: "wss://ws.api.prod.paradex.trade/v1".to_owned(),
        binance_ws: "wss://fstream.binance.com".to_owned(),
        binance_rest: "https://fapi.binance.com".to_owned(),
    };
    assert_eq!(parsed, Ok(Parsed::Watch(Box::new(want))));

    let parsed = args::parse(strings(&[
        "--binance",
        "BTCUSDT",
        "--socks5",
        "127.0.0.1:1080",
        "--top",
        "15",
        "--binance-tick",
        "0.1",
        "--binance-step",
        "0.001",
    ]));
    let Ok(Parsed::Watch(opts)) = parsed else {
        panic!("{parsed:?}")
    };
    assert_eq!(
        opts.proxy,
        ProxyConfig::Socks5 {
            host: "127.0.0.1".to_owned(),
            port: 1080
        }
    );
    assert_eq!(opts.top, 15);
    let binance = opts.binance.unwrap();
    assert_eq!(
        (binance.tick, binance.step),
        (Decimal::new(1, 1), Decimal::new(1, 3))
    );
    assert_eq!(opts.paradex, None);

    // RA98-2: Paradex spells some markets with a lowercase prefix, as the Java library's
    // signing vectors do (`kBONK-USD-PERP`), and passes the name through as typed.
    let parsed = args::parse(strings(&["--paradex", "kBONK-USD-PERP"]));
    let Ok(Parsed::Watch(opts)) = parsed else {
        panic!("{parsed:?}")
    };
    assert_eq!(opts.paradex.unwrap().symbol, "kBONK-USD-PERP");

    // FBC-taxd: which of the Paradex market's touches; bbo when not given.
    for (which, want) in [
        ("bbo", ParadexTouch::Bbo),
        ("interactive", ParadexTouch::Interactive),
        ("both", ParadexTouch::Both),
    ] {
        let parsed = args::parse(strings(&["--paradex", "X", "--paradex-touch", which]));
        let Ok(Parsed::Watch(opts)) = parsed else {
            panic!("{parsed:?}")
        };
        assert_eq!(opts.paradex_touch, want);
    }

    for (args, why) in [
        (&[][..], "name a market"),
        (&["--seconds", "5"][..], "name a market"),
        (&["--paradex"][..], "--paradex needs a value"),
        (
            &["--paradex", "X", "--seconds", "0"][..],
            "not a positive whole number",
        ),
        (
            &["--paradex", "X", "--top", "16"][..],
            "not a whole number from 0 to 15",
        ),
        (
            &["--paradex", "X", "--socks5", "proxy"][..],
            "not host:port",
        ),
        (
            &["--paradex", "X", "--socks5", ":1080"][..],
            "not host:port",
        ),
        (
            &["--paradex", "X", "--socks5", "proxy:0"][..],
            "not host:port",
        ),
        (
            &["--paradex", "X", "--paradex-tick", "0"][..],
            "not a positive decimal",
        ),
        (
            &["--paradex", "X", "--key", "k"][..],
            "unknown argument --key",
        ),
        // RB98-1: Binance's frames name the symbol in capitals, so a lowercase one would
        // subscribe and then have every frame refused unseen.
        (
            &["--binance", "btcusdt"][..],
            "--binance btcusdt: not a market as the venue spells it",
        ),
        // RA98-2: the capitals rule stays for Binance, whose frames name the symbol in capitals.
        (
            &["--binance", "kBONKUSDT"][..],
            "--binance kBONKUSDT: not a market as the venue spells it",
        ),
        (
            &["--paradex", "BTC-USD-PERP "][..],
            "not a market as the venue spells it",
        ),
        (
            &["--paradex", "BTC_USD_PERP"][..],
            "not a market as the venue spells it",
        ),
        (
            &["--paradex", "X", "--paradex-touch", "rpi"][..],
            "--paradex-touch rpi: not bbo, interactive or both",
        ),
        // A flag where a value belongs: the value was left out.
        (&["--binance", "--top", "2"][..], "--binance needs a value"),
        (
            &["--paradex", "X", "--paradex-url", "--seconds", "5"][..],
            "--paradex-url needs a value",
        ),
    ] {
        let err = args::parse(strings(args)).unwrap_err();
        assert!(err.contains(why), "{args:?}: {err}");
    }
}

/// Standard output that takes the first line, then fails every write with `kind`: a broken
/// pipe is what `| head` leaves, a full disk what `> watch.log` can.
struct FailsAfterFirstLine {
    taken: Rc<RefCell<Vec<u8>>>,
    kind: std::io::ErrorKind,
}

impl std::io::Write for FailsAfterFirstLine {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut taken = self.taken.borrow_mut();
        if taken.contains(&b'\n') {
            return Err(self.kind.into());
        }
        taken.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Runs md_watch, with no stop of its own, against a stub that sends Paradex's subscribe acks
/// and a bbo frame, writing to standard output that fails with `kind` after the header line.
/// Returns what `run` returned and the bytes standard output took.
async fn run_until_standard_output_fails(kind: std::io::ErrorKind) -> (Result<(), String>, String) {
    let mut steps = vec![Step::Accept];
    steps.extend((0..3).map(|_| Step::Read { conn: 0 }));
    steps.extend((1..=3).map(|id| Step::Push {
        conn: 0,
        frame: Frame::text(format!(r#"{{"jsonrpc":"2.0","result":{{}},"id":{id}}}"#)),
    }));
    steps.push(Step::Push {
        conn: 0,
        frame: sbe("bbo.sbe.txt"),
    });
    let paradex = StubServer::start(WsScript::new(steps), HttpRoutes::new())
        .await
        .unwrap();
    let parsed = args::parse(strings(&[
        "--paradex",
        "BTC-USD-PERP",
        "--paradex-url",
        &paradex.ws_url("/v1"),
    ]))
    .unwrap();
    let Parsed::Watch(opts) = parsed else {
        panic!("not a watch: {parsed:?}")
    };
    let taken = Rc::new(RefCell::new(Vec::new()));
    let out: watch::Out = Rc::new(RefCell::new(FailsAfterFirstLine {
        taken: taken.clone(),
        kind,
    }));
    let run = watch::run(&opts, out, false, std::future::pending());
    let Ok(ran) = tokio::time::timeout(Duration::from_secs(60), run).await else {
        panic!("md_watch kept running for 60 s after its standard output failed ({kind:?})")
    };
    paradex.finished().await.unwrap();
    assert_eq!(paradex.connections().len(), 1);
    let taken = String::from_utf8(taken.borrow().clone()).unwrap();
    (ran, taken)
}

const HEADER: &str = "md_watch: paradex BTC-USD-PERP (bbo, trades, deltas book); market data \
                      only, no order is ever sent\n";

/// RB98-4: `md_watch ... | head` ends. Run with no stop of its own, md_watch prints its header,
/// and the first line the venue's frames call for fails to write: md_watch stops, closing the
/// connection, instead of watching forever with nowhere to print. A closed pipe is the reader
/// leaving, not a failure, so md_watch ends quietly.
#[tokio::test]
async fn md_watch_stops_once_standard_output_is_closed() {
    let (ran, taken) = run_until_standard_output_fails(std::io::ErrorKind::BrokenPipe).await;
    assert_eq!(ran, Ok(()));
    assert_eq!(taken, HEADER);
}

/// RB98-6: any other write error (a full disk under `> watch.log`) also stops md_watch, and is
/// returned as a failure naming standard output, so the log's end is explained and the exit
/// status is not success.
#[tokio::test]
async fn md_watch_reports_a_write_error_other_than_a_closed_pipe() {
    let (ran, taken) = run_until_standard_output_fails(std::io::ErrorKind::StorageFull).await;
    let err = ran.expect_err("a full disk ended md_watch as a success");
    assert!(err.starts_with("standard output: "), "{err}");
    assert_eq!(taken, HEADER);
}

#[test]
fn a_line_is_stamped_with_its_utc_time_to_the_millisecond() {
    // 2023-11-14 22:13:20.123456789 UTC.
    assert_eq!(
        watch::clock(WallNs(1_700_000_000_123_456_789)),
        "22:13:20.123"
    );
    assert_eq!(watch::clock(WallNs(0)), "00:00:00.000");
}

/// The binary frames of the Paradex capture `name` (`fixtures/paradex/md/`, one JSON object
/// per line, a binary frame's bytes in standard base64 in `b64`), after its acknowledgement.
fn captured(name: &str) -> Vec<Frame> {
    let path = fixture_path(&format!("paradex/md/{name}"));
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let base64 = |text: &str| {
        let value = |c: u8| match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("not base64: {c}"),
        };
        let (mut out, mut acc, mut bits) = (Vec::new(), 0u32, 0u32);
        for &c in text.trim_end_matches('=').as_bytes() {
            acc = (acc << 6) | u32::from(value(c));
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
            }
        }
        out
    };
    // `"b64": "<digits>"`: the acknowledgement, a text frame, has none.
    text.lines()
        .filter_map(|line| {
            let (_, rest) = line.split_once(r#""b64": ""#)?;
            let (b64, _) = rest.split_once('"').unwrap();
            Some(Frame::Binary(base64(b64)))
        })
        .collect()
}

/// FBC-taxd: `--paradex-touch interactive` subscribes the RPI-inclusive touch,
/// `bbo.<MARKET>.interactive`, in place of the plain bbo, and prints its frames (the first two
/// of the 2026-10-08 production capture) under its own channel name, `bbo.interactive`. The
/// interactive touch has the second connection to itself, the trades and the book the first
/// (Codex r4214305885); the two may be accepted in either order, so the stub answers each
/// connection's first subscribe by what it names: an acknowledgement, and the captured frames
/// after the interactive touch's.
#[tokio::test]
async fn md_watch_prints_the_interactive_touch_under_its_own_channel_name() {
    let frames = captured("btc-bbo-interactive-2026-10-08.jsonl");
    let first_two: Vec<Frame> = frames.into_iter().take(2).collect();
    let answer = Responder::new(move |frame| {
        let Frame::Text(text) = frame else {
            return Err("a subscribe is a text frame".to_owned());
        };
        let field = |name: &str, end: char| {
            let (_, rest) = text.split_once(name)?;
            Some(rest.split_once(end)?.0.to_owned())
        };
        let id = field(r#""id":"#, ',').or_else(|| field(r#""id":"#, '}'));
        let channel = field(r#""channel":""#, '"').ok_or("no channel")?;
        let id = id.ok_or("no id")?;
        let mut out = vec![Frame::text(format!(
            r#"{{"jsonrpc":"2.0","result":{{"channel":"{channel}"}},"id":{id}}}"#
        ))];
        if channel.ends_with(".interactive") {
            out.extend(first_two.iter().cloned());
        }
        Ok(out)
    });
    let steps = vec![
        Step::Accept,
        Step::Accept,
        Step::Respond {
            conn: 0,
            with: answer.clone(),
        },
        Step::Respond {
            conn: 1,
            with: answer,
        },
    ];
    let paradex = StubServer::start(WsScript::new(steps), HttpRoutes::new())
        .await
        .unwrap();
    let parsed = args::parse(strings(&[
        "--paradex",
        "BTC-USD-PERP",
        "--paradex-touch",
        "interactive",
        "--paradex-url",
        &paradex.ws_url("/v1"),
    ]))
    .unwrap();
    let Parsed::Watch(opts) = parsed else {
        panic!("not a watch: {parsed:?}")
    };
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: watch::Out = buf.clone();
    let text = || String::from_utf8(buf.borrow().clone()).unwrap();
    let second = "TOUCH paradex BTC-USD-PERP bbo.interactive bid 83422.7 x 0.00015 \
                  ask 83442.3 x 0.00015 mid 83432.5 spread 2.35bps";
    let stop = async {
        paradex.finished().await.unwrap();
        while !text().contains(second) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let run = watch::run(&opts, out, false, stop);
    let Ok(ran) = tokio::time::timeout(Duration::from_secs(60), run).await else {
        panic!(
            "md_watch did not print the interactive touches within 60 s:\n{}",
            text()
        )
    };
    ran.unwrap();
    let printed = text();
    let lines: Vec<&str> = printed.lines().collect();
    assert_eq!(
        lines[..3],
        [
            "md_watch: paradex BTC-USD-PERP (bbo.interactive, trades, deltas book); market \
             data only, no order is ever sent",
            "TOUCH paradex BTC-USD-PERP bbo.interactive bid 83422.7 x 0.00015 \
             ask 83452.9 x 0.00031 mid 83437.8 spread 3.62bps",
            second,
        ]
    );
    // Then md_watch's stop closes both connections, in either order.
    let mut closed = lines[3..].to_vec();
    closed.sort_unstable();
    assert_eq!(
        closed,
        [
            "CLOSED paradex conn 0 epoch 0 ended",
            "CLOSED paradex conn 1 epoch 0 ended",
        ]
    );
    // The connection carrying the interactive touch subscribed exactly
    // bbo.BTC-USD-PERP.interactive and nothing else; no connection subscribed the plain bbo.
    let subscribes = |conn: &fbc_conformance::ConnRecord| -> Vec<String> {
        conn.received
            .iter()
            .filter_map(|frame| match frame {
                Frame::Text(t) if t.contains(r#""method":"subscribe""#) => {
                    let (_, rest) = t.split_once(r#""channel":""#).unwrap();
                    Some(rest.split_once('"').unwrap().0.to_owned())
                }
                _ => None,
            })
            .collect()
    };
    let conns = paradex.connections();
    assert_eq!(conns.len(), 2);
    let channels: Vec<Vec<String>> = conns.iter().map(subscribes).collect();
    let interactive = ["bbo.BTC-USD-PERP.interactive".to_owned()];
    assert!(
        channels.iter().any(|c| c[..] == interactive),
        "{channels:?}"
    );
    assert!(
        channels.iter().flatten().all(|c| c != "bbo.BTC-USD-PERP"),
        "{channels:?}"
    );
}

/// FBC-taxd: `--paradex-touch both` subscribes the plain and the interactive touch, each under
/// its own channel name; the venue plans them on two connections (the Paradex crate's
/// `tests/md_touch_interactive.rs`).
#[test]
fn both_paradex_touches_are_named_apart() {
    use fbc_venue_paradex::md::{BBO, BBO_INTERACTIVE};
    assert_eq!(watch::paradex_touches(ParadexTouch::Bbo), [(BBO, "bbo")]);
    assert_eq!(
        watch::paradex_touches(ParadexTouch::Interactive),
        [(BBO_INTERACTIVE, "bbo.interactive")]
    );
    assert_eq!(
        watch::paradex_touches(ParadexTouch::Both),
        [(BBO, "bbo"), (BBO_INTERACTIVE, "bbo.interactive")]
    );
}

// FBC-9o7e: Hibachi.

/// The hand-built Hibachi frame `fixtures/hibachi/md/<name>.json`, without its line end.
fn hibachi_frame(name: &str) -> Frame {
    let path = fixture_path(&format!("hibachi/md/{name}.json"));
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    Frame::text(text.trim_end())
}

/// The stub's `GET /market/exchange-info`: the hand-built answer listing ETH/USDT-P (tick
/// 0.000001, step 0.000000001, quote grid 0.01), EUR/USDT-P (an FX pair), GBP/USDT-P (`PAUSED`)
/// and other contracts, but no BTC/USDT-P.
fn exchange_info() -> HttpRoutes {
    let path = fixture_path("hibachi/rest/exchange_info.json");
    let body = fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    HttpRoutes::from([(
        "/market/exchange-info".to_owned(),
        HttpReply { status: 200, body },
    )])
}

/// md_watch's command line for `--hibachi <symbol>` against `stub`, at the fixtures' depth of
/// 3, with `more` after it.
fn hibachi_args(stub: &StubServer, symbol: &str, more: &[&str]) -> Box<Options> {
    let (rest, ws) = (stub.http_url(""), stub.ws_url(""));
    let mut argv = vec![
        "--hibachi",
        symbol,
        "--hibachi-rest",
        &rest,
        "--hibachi-ws",
        &ws,
    ];
    argv.extend(["--hibachi-depth", "3"]);
    argv.extend(more);
    match args::parse(strings(&argv)).unwrap() {
        Parsed::Watch(opts) => opts,
        Parsed::Help => panic!("not a watch"),
    }
}

/// The subscribe and unsubscribe frames a connection received, as `<method> <topic>` and, for
/// an orderbook subscribe, its granularity and depth.
fn hibachi_requests(conn: &fbc_conformance::ConnRecord) -> Vec<String> {
    conn.received
        .iter()
        .map(|frame| {
            let Frame::Text(text) = frame else {
                panic!("md_watch sent Hibachi a binary frame")
            };
            let v: serde_json::Value = serde_json::from_str(text).unwrap();
            let sub = &v["parameters"]["subscriptions"][0];
            let mut said = format!("{} {}", v["method"].as_str().unwrap(), sub["topic"]);
            if let Some(granularity) = sub.get("granularity") {
                said.push_str(&format!(
                    " granularity {granularity} depth {}",
                    sub["depth"]
                ));
            }
            assert_eq!(sub["symbol"], "ETH/USDT-P", "{text}");
            said
        })
        .collect()
}

/// `HH:MM:SS.mmm `, the receive time md_watch leads a line with when it is run.
fn stamped(line: &str) -> Option<&str> {
    let (stamp, rest) = line.split_at_checked(13)?;
    let b = stamp.as_bytes();
    let digits = [0, 1, 3, 4, 6, 7, 9, 10, 11];
    let shaped = digits.iter().all(|&i| b[i].is_ascii_digit())
        && (b[2], b[5], b[8], b[12]) == (b':', b':', b'.', b' ');
    shaped.then_some(rest)
}

/// What the stub's ETH/USDT-P shows on the status poll (decision 0130): `LIVE` on a `NORMAL`
/// exchange, so trading.
const ETH_TRADING: &str = "STATUS hibachi ETH/USDT-P trading";
/// The status poll's connection, the one after the socket's (128) in Hibachi's range: its end
/// prints no `COUNTS`, which only the socket's codec counts.
const POLL_CLOSED: &str = "CLOSED hibachi conn 129 epoch 0 ended";

/// `lines` without the status poll's (FBC-1632, decision 0130): its `STATUS` line, which comes
/// whenever the poll's first read is answered, so at no fixed place among the socket's lines (and
/// not at all when md_watch stops first), and its connection's `CLOSED` line, which ends every
/// run. Each is checked: at most one `STATUS` line, ETH/USDT-P trading, and the poll's
/// connection closed exactly once, at its first epoch.
fn socket_lines(lines: &[String]) -> Vec<String> {
    let (mut statuses, mut closed) = (0, 0);
    let mut kept = Vec::new();
    for line in lines {
        let bare = stamped(line).unwrap_or(line);
        if bare.starts_with("STATUS ") {
            assert_eq!(bare, ETH_TRADING, "{lines:#?}");
            statuses += 1;
        } else if bare == POLL_CLOSED {
            closed += 1;
        } else {
            kept.push(line.clone());
        }
    }
    assert!(statuses <= 1, "{lines:#?}");
    assert_eq!(closed, 1, "{lines:#?}");
    kept
}

/// Runs md_watch on `opts` against `stub` until `done` holds of what it printed (bounded at 60 s,
/// so a regression fails instead of hanging), each line led by its receive time when `stamps`.
async fn watch_until(opts: &Options, stamps: bool, done: impl Fn(&str) -> bool) -> Vec<String> {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: watch::Out = buf.clone();
    let text = || String::from_utf8(buf.borrow().clone()).unwrap();
    let stop = async {
        while !done(&text()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let run = watch::run(opts, out, stamps, stop);
    let Ok(ran) = tokio::time::timeout(Duration::from_secs(60), run).await else {
        panic!(
            "md_watch did not print what was waited for within 60 s:\n{}",
            text()
        )
    };
    ran.unwrap();
    text().lines().map(str::to_owned).collect()
}

/// FBC-9o7e's done line: `--hibachi ETH/USDT-P` reads the contract from the stub's
/// `GET /market/exchange-info`, then subscribes `ask_bid_price`, `trades` and the `orderbook` at
/// the contract's quote grid (0.01) and the given depth on `/ws/market`, and prints the
/// hand-built frames: the orderbook Snapshot `eth_s0` as the book's touch and its top three
/// levels, the sized `ask_bid_price` frame `abp_sized` as the venue's touch, apart from the
/// book's (it sits inside the book, on the finer tick: 2000.012345 against the book's bucket
/// 2000.00), the Update `eth_u1` (2000.10 emptied, 2000.40 scrolled in) and the trade. Run as the
/// owner runs it, every line but the first and the level rows is led by its receive time. The
/// contract read and the one connection went to the stub and nowhere else; the codec counted
/// nothing it passed over (`COUNTS ... none`) before md_watch's stop closed the connection.
#[tokio::test]
async fn md_watch_prints_hibachis_ask_bid_price_touch_and_its_orderbooks_touch_and_top_of_book() {
    let mut steps = vec![Step::Accept];
    steps.extend((0..3).map(|_| Step::Read { conn: 0 }));
    for name in ["eth_s0", "abp_sized", "eth_u1", "trade_single"] {
        steps.push(Step::Push {
            conn: 0,
            frame: hibachi_frame(name),
        });
    }
    let stub = StubServer::start(WsScript::new(steps), exchange_info())
        .await
        .unwrap();
    let opts = hibachi_args(&stub, "ETH/USDT-P", &["--top", "3"]);
    let trade = "TRADE hibachi ETH/USDT-P buy 0.25 @ 2000.051234";
    // Stopped once the trade and the status poll's first answer are both printed.
    let lines = watch_until(&opts, true, |t| {
        t.contains(trade) && t.contains(ETH_TRADING)
    })
    .await;
    stub.finished().await.unwrap();
    // FBC-1632: the status poll Hibachi plans beside the socket printed the contract's status
    // once, and its connection closed with no COUNTS line of its own.
    let statuses = lines.iter().filter(|l| l.ends_with(ETH_TRADING)).count();
    assert_eq!(statuses, 1, "{lines:#?}");
    let lines = socket_lines(&lines);

    assert_eq!(
        lines[0],
        "md_watch: hibachi ETH/USDT-P (ask_bid_price, trades, orderbook book of 3 levels at \
         granularity 0.01); market data only, no order is ever sent"
    );
    // Each line md_watch prints is led by its receive time, the level rows under a book line
    // excepted, which belong to it.
    let mut unstamped = Vec::new();
    for line in &lines[1..] {
        if line.starts_with("  ") {
            unstamped.push(line.as_str());
        } else {
            let rest = stamped(line).unwrap_or_else(|| panic!("not stamped: {line}"));
            unstamped.push(rest);
        }
    }
    let book = "BOOK hibachi ETH/USDT-P orderbook";
    assert_eq!(
        unstamped,
        [
            "MARKET hibachi ETH/USDT-P open",
            &format!("{book} bid 2000 x 1.25 ask 2000.1 x 1.5 mid 2000.05 spread 0.50bps"),
            "  1 bid 2000 x 1.25 | ask 2000.1 x 1.5",
            "  2 bid 1999.9 x 3 | ask 2000.2 x 2",
            "  3 bid 1999.8 x 0.5 | ask 2000.3 x 0.75",
            "TOUCH hibachi ETH/USDT-P ask_bid_price bid 2000.012345 x 1.25 ask 2000.104321 x 1.5 \
             mid 2000.058333 spread 0.46bps",
            &format!("{book} bid 2000 x 1.25 ask 2000.2 x 2 mid 2000.1 spread 1.00bps"),
            "  1 bid 2000 x 1.25 | ask 2000.2 x 2",
            "  2 bid 1999.9 x 3 | ask 2000.3 x 0.75",
            "  3 bid 1999.8 x 0.5 | ask 2000.4 x 1.1",
            trade,
            "COUNTS hibachi ETH/USDT-P none",
            "CLOSED hibachi conn 128 epoch 0 ended",
        ]
    );
    // The contract was read from the stub, then once more by the status poll (its interval, 60 s,
    // had not come round again), and the one connection was the stub's: md_watch subscribed each
    // topic once, the orderbook at the contract's quote grid and the depth given.
    assert_eq!(stub.http_requests(), ["GET /market/exchange-info"; 2]);
    let conns = stub.connections();
    assert_eq!(conns.len(), 1);
    let mut asked = hibachi_requests(&conns[0]);
    asked.sort_unstable();
    assert_eq!(
        asked,
        [
            r#"subscribe "ask_bid_price""#,
            r#"subscribe "orderbook" granularity "0.01" depth 3"#,
            r#"subscribe "trades""#,
        ]
    );
}

/// PR #210's open point, as the owner's first live run reads it (the crate's README): when
/// Hibachi does not honour the orderbook subscription's `granularity` and `depth`, every
/// Snapshot comes back at the venue's own and gaps the book. Here the first Snapshot states a
/// granularity of 0.1 (`eth_granularity_differs`) and, after the codec's resubscribe, the next a
/// depth of 5 (`eth_depth_differs`), against the subscription's 0.01 and 3: each prints a
/// `HEALTH ... orderbook gap` and no book, the codec's counts are printed as it first counts
/// each kind (`param_mismatches`, then `resubscribes`), and the closing `COUNTS` line names
/// both mismatches and the one resubscribe. The stub saw the codec unsubscribe the orderbook
/// and subscribe it again with the same granularity and depth.
#[tokio::test]
async fn a_hibachi_book_whose_snapshots_ignore_the_subscriptions_fields_gaps_with_param_mismatches_counted()
 {
    let mut steps = vec![Step::Accept];
    steps.extend((0..3).map(|_| Step::Read { conn: 0 }));
    steps.push(Step::Push {
        conn: 0,
        frame: hibachi_frame("eth_granularity_differs"),
    });
    // The resubscribe, after --hibachi-retry: an unsubscribe and a subscribe.
    steps.extend((0..2).map(|_| Step::Read { conn: 0 }));
    steps.push(Step::Push {
        conn: 0,
        frame: hibachi_frame("eth_depth_differs"),
    });
    let stub = StubServer::start(WsScript::new(steps), exchange_info())
        .await
        .unwrap();
    // The next resubscribe would come a second after the second gap: md_watch stops before it.
    let opts = hibachi_args(&stub, "ETH/USDT-P", &["--hibachi-retry", "1000"]);
    let gap = "HEALTH hibachi ETH/USDT-P orderbook gap";
    let lines = watch_until(&opts, false, |t| {
        t.matches(gap).count() == 2 && t.contains("resubscribes 1")
    })
    .await;
    let lines = socket_lines(&lines);
    stub.finished().await.unwrap();

    let hibachi = of(&lines[1..], "hibachi");
    assert_eq!(hibachi[0], "MARKET hibachi ETH/USDT-P open");
    assert!(
        hibachi.iter().all(|l| !l.starts_with("BOOK ")),
        "{hibachi:#?}"
    );
    assert_eq!(
        hibachi.iter().filter(|l| *l == gap).count(),
        2,
        "{hibachi:#?}"
    );
    let counts: Vec<&str> = hibachi
        .iter()
        .filter(|l| l.starts_with("COUNTS "))
        .map(String::as_str)
        .collect();
    // Printed as each kind is first counted (the second only once the resubscribe is counted,
    // after or before the second Snapshot's mismatch), then in full before the connection's end.
    let first = "COUNTS hibachi ETH/USDT-P param_mismatches 1";
    let last = "COUNTS hibachi ETH/USDT-P param_mismatches 2 resubscribes 1";
    assert_eq!(counts.first(), Some(&first), "{hibachi:#?}");
    assert_eq!(counts.len(), 3, "{hibachi:#?}");
    assert!(counts[1].ends_with(" resubscribes 1"), "{hibachi:#?}");
    let n = hibachi.len();
    assert_eq!(
        hibachi[n - 2..],
        [last, "CLOSED hibachi conn 128 epoch 0 ended"]
    );

    let conns = stub.connections();
    assert_eq!(conns.len(), 1);
    let asked = hibachi_requests(&conns[0]);
    assert_eq!(
        asked[3..],
        [
            r#"unsubscribe "orderbook""#,
            r#"subscribe "orderbook" granularity "0.01" depth 3"#,
        ]
    );
}

/// The crate README's account of how the owner's first live run reads, outcome by outcome: each
/// `- **<label>**:` bullet of its md_watch section, its wrapped lines joined into one.
fn readme_outcomes() -> Vec<(String, String)> {
    let readme = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let readme = fs::read_to_string(readme).unwrap();
    let section = readme
        .split("\n## md_watch\n")
        .nth(1)
        .expect("an md_watch section");
    let section = section.split("\n## ").next().unwrap();
    let mut outcomes: Vec<(String, String)> = Vec::new();
    let mut inside = false;
    for line in section.lines() {
        if let Some(rest) = line.strip_prefix("- **") {
            let (label, text) = rest.split_once("**:").expect("a labelled outcome");
            outcomes.push((label.to_owned(), text.trim().to_owned()));
            inside = true;
        } else if inside && line.starts_with("  ") {
            let text = &mut outcomes.last_mut().unwrap().1;
            text.push(' ');
            text.push_str(line.trim());
        } else {
            inside = false;
        }
    }
    outcomes
}

/// The README's outcome `label`, its lines joined.
fn readme_outcome(label: &str) -> String {
    let found = readme_outcomes().into_iter().find(|(l, _)| l == label);
    found
        .map(|(_, text)| text)
        .unwrap_or_else(|| panic!("the README gives no outcome {label:?}"))
}

/// Reviewer B RB213-1: a book whose Snapshots come back at the subscription's own granularity
/// and depth but fail another of the codec's checks gaps on each Snapshot, the check's own
/// counter named and never `param_mismatches`, and every resubscribe is answered, so no
/// reconnect is asked for. The stub answers the orderbook subscription, and each of the codec's
/// two resubscribes, with `eth_snapshot_thin` (granularity 0.01 and depth 3, as asked, but two
/// asks): each prints `HEALTH ... orderbook gap` and no book, and `COUNTS` names `thin_sides`,
/// then `resubscribes`. The README reads such a run as an outcome of its own, which names that
/// counter and says how a thin side is told from a shallow live book; "Not answered", whose
/// opening sentence these lines once fitted, now asks for what they lack.
#[tokio::test]
async fn a_hibachi_book_whose_snapshots_take_the_subscriptions_fields_but_are_thin_gaps_with_thin_sides_counted()
 {
    let mut steps = vec![Step::Accept];
    steps.extend((0..3).map(|_| Step::Read { conn: 0 }));
    for resubscribed in [false, true, true] {
        if resubscribed {
            // The resubscribe, after --hibachi-retry: an unsubscribe and a subscribe.
            steps.extend((0..2).map(|_| Step::Read { conn: 0 }));
        }
        steps.push(Step::Push {
            conn: 0,
            frame: hibachi_frame("eth_snapshot_thin"),
        });
    }
    let stub = StubServer::start(WsScript::new(steps), exchange_info())
        .await
        .unwrap();
    // The next resubscribe would come a second after the third gap: md_watch stops before it.
    let opts = hibachi_args(&stub, "ETH/USDT-P", &["--hibachi-retry", "1000"]);
    let gap = "HEALTH hibachi ETH/USDT-P orderbook gap";
    let lines = watch_until(&opts, false, |t| t.matches(gap).count() == 3).await;
    let lines = socket_lines(&lines);
    stub.finished().await.unwrap();

    let hibachi = of(&lines[1..], "hibachi");
    assert_eq!(hibachi[0], "MARKET hibachi ETH/USDT-P open");
    assert_eq!(
        hibachi.iter().filter(|l| *l == gap).count(),
        3,
        "{hibachi:#?}"
    );
    let shown = |l: &&String| l.starts_with("BOOK ") || l.starts_with("RECONNECT ");
    assert!(!hibachi.iter().any(|l| shown(&l)), "{hibachi:#?}");
    let counts: Vec<&str> = hibachi
        .iter()
        .filter(|l| l.starts_with("COUNTS "))
        .map(String::as_str)
        .collect();
    // Printed as each kind is first counted (the first resubscribe before or after the second
    // Snapshot's gap), then in full before the connection's end.
    let first = "COUNTS hibachi ETH/USDT-P thin_sides 1";
    let resubscribed = [
        "COUNTS hibachi ETH/USDT-P thin_sides 1 resubscribes 1",
        "COUNTS hibachi ETH/USDT-P thin_sides 2 resubscribes 1",
    ];
    let last = "COUNTS hibachi ETH/USDT-P thin_sides 3 resubscribes 2";
    assert_eq!(counts.len(), 3, "{hibachi:#?}");
    assert_eq!(counts[0], first, "{hibachi:#?}");
    assert!(resubscribed.contains(&counts[1]), "{hibachi:#?}");
    let n = hibachi.len();
    assert_eq!(
        hibachi[n - 2..],
        [last, "CLOSED hibachi conn 128 epoch 0 ended"]
    );
    // Each Snapshot stated the subscription's own fields, and the codec asked for the same ones
    // again on each resubscribe.
    let conns = stub.connections();
    assert_eq!(conns.len(), 1);
    let asked = hibachi_requests(&conns[0]);
    let again = [
        r#"unsubscribe "orderbook""#,
        r#"subscribe "orderbook" granularity "0.01" depth 3"#,
    ];
    assert_eq!(asked[3..], [again, again].concat());

    // The README's outcome for these lines names their counter, the counters they lack, and the
    // smaller depth that tells a thin side from a shallow live book.
    let taken = readme_outcome("Taken, but the book fails another check");
    for said in [
        "`thin_sides`",
        "no `param_mismatches`",
        "no `reconnects`",
        "a smaller `--hibachi-depth`",
    ] {
        assert!(
            taken.contains(said),
            "the outcome does not say {said}: {taken}"
        );
    }
    // "Not answered" asks for a RECONNECT on every connection and no orderbook counter but the
    // two these lines would have to show, so it cannot be read into them.
    let not_answered = readme_outcome("Not answered");
    for said in [
        "a `RECONNECT` line follows on every connection",
        "no orderbook counter but `resubscribes` and `reconnects`",
        "`thin_sides`",
    ] {
        assert!(
            not_answered.contains(said),
            "\"Not answered\" does not say {said}: {not_answered}"
        );
    }
}

/// Reviewer B RB213-1: "Not answered" as the README now gives it. No orderbook frame comes, so
/// the book's first gap is printed only when the codec's wait for its Snapshot runs out, on no
/// frame; the codec then resubscribes and, that unanswered too, asks for a reconnect. `COUNTS`
/// names `resubscribes` and `reconnects` and no orderbook counter besides, and a `RECONNECT`
/// line ends the connection. The stub reads the first connection's three subscriptions and the
/// resubscribe and answers none of them, then the second connection's subscriptions.
#[tokio::test]
async fn a_hibachi_orderbook_subscription_left_unanswered_gaps_resubscribes_then_reconnects() {
    let mut steps = vec![Step::Accept];
    steps.extend((0..5).map(|_| Step::Read { conn: 0 }));
    steps.push(Step::Accept);
    steps.extend((0..3).map(|_| Step::Read { conn: 1 }));
    let stub = StubServer::start(WsScript::new(steps), exchange_info())
        .await
        .unwrap();
    // The second connection's wait would end a second after it subscribes: md_watch stops before.
    let opts = hibachi_args(&stub, "ETH/USDT-P", &["--hibachi-retry", "1000"]);
    let resubscribed = |_: &str| {
        let conns = stub.connections();
        conns.get(1).is_some_and(|c| c.received.len() == 3)
    };
    let lines = watch_until(&opts, false, resubscribed).await;
    let lines = socket_lines(&lines);
    stub.finished().await.unwrap();

    let hibachi = of(&lines[1..], "hibachi");
    let at = hibachi
        .iter()
        .position(|l| l.starts_with("RECONNECT "))
        .unwrap_or_else(|| panic!("no reconnect: {hibachi:#?}"));
    let (first, second) = hibachi.split_at(at);
    let both = "COUNTS hibachi ETH/USDT-P resubscribes 1 reconnects 1";
    // Printed as first counted when a flush came between the reconnect asked for and the
    // connection's end, and in full at its end.
    assert!((4..=5).contains(&first.len()), "{hibachi:#?}");
    assert_eq!(
        first[..3],
        [
            "MARKET hibachi ETH/USDT-P open",
            "HEALTH hibachi ETH/USDT-P orderbook gap",
            "COUNTS hibachi ETH/USDT-P resubscribes 1",
        ],
        "{hibachi:#?}"
    );
    assert!(first[3..].iter().all(|l| l == both), "{hibachi:#?}");
    assert_eq!(
        second,
        [
            "RECONNECT hibachi conn 128 epoch 0 ended",
            "COUNTS hibachi ETH/USDT-P none",
            "CLOSED hibachi conn 128 epoch 1 ended",
        ],
        "{hibachi:#?}"
    );
    let conns = stub.connections();
    let asked = hibachi_requests(&conns[0]);
    assert_eq!(
        asked[3..],
        [
            r#"unsubscribe "orderbook""#,
            r#"subscribe "orderbook" granularity "0.01" depth 3"#,
        ]
    );
}

/// md_watch prints the codec's counts per connection, through a real reconnect: in full before
/// each `RECONNECT` and `CLOSED` line, and on the second connection as first counted there,
/// though the first counted the same kind. The stub sends a price-only `ask_bid_price` frame
/// (`abp_price_only`, counted `unsized_touches`, no touch in force so no gap) on the first
/// connection and closes it, then the same frame on the second; no Snapshot comes, and the
/// orderbook's wait for one outlasts the run. Whether a flush prints the first connection's
/// count before the stub's close is a matter of timing; that a count printed there does not
/// keep the second connection's from printing is `watch.rs`'s
/// `a_hibachi_codecs_counts_are_printed_afresh_after_each_reconnect`.
#[tokio::test]
async fn md_watch_prints_a_hibachi_codecs_counts_afresh_on_each_connection() {
    let mut steps = Vec::new();
    for conn in [0, 1] {
        steps.push(Step::Accept);
        steps.extend((0..3).map(|_| Step::Read { conn }));
        steps.push(Step::Push {
            conn,
            frame: hibachi_frame("abp_price_only"),
        });
        if conn == 0 {
            steps.push(Step::Close { conn });
        }
    }
    let stub = StubServer::start(WsScript::new(steps), exchange_info())
        .await
        .unwrap();
    let opts = hibachi_args(&stub, "ETH/USDT-P", &["--hibachi-retry", "60000"]);
    let counted = "COUNTS hibachi ETH/USDT-P unsized_touches 1";
    // Stopped once the second connection's count is printed as first counted there.
    let lines = watch_until(&opts, false, |t| {
        t.split_once("RECONNECT hibachi")
            .is_some_and(|(_, after)| after.contains(counted))
    })
    .await;
    let lines = socket_lines(&lines);
    stub.finished().await.unwrap();

    let hibachi = of(&lines[1..], "hibachi");
    let at = hibachi
        .iter()
        .position(|l| l.starts_with("RECONNECT "))
        .unwrap_or_else(|| panic!("no reconnect: {hibachi:#?}"));
    let (first, second) = hibachi.split_at(at);
    // The first connection's count, printed as first counted when a flush came before the
    // close, and in full at its end.
    assert_eq!(first[0], "MARKET hibachi ETH/USDT-P open");
    assert!(
        (2..=3).contains(&first.len()) && first[1..].iter().all(|l| l == counted),
        "{hibachi:#?}"
    );
    assert_eq!(
        second,
        [
            "RECONNECT hibachi conn 128 epoch 0 ended",
            counted,
            counted,
            "CLOSED hibachi conn 128 epoch 1 ended",
        ],
        "{hibachi:#?}"
    );
    assert_eq!(stub.connections().len(), 2);
}

/// FBC-9o7e: `--hibachi` takes a contract as Hibachi spells it, and every other value
/// `fbc-venue-hibachi`'s configuration requires has a default the usage text states (its
/// origins, the read's timeout, the depth and the resubscribe interval), which the venue's own
/// configuration check accepts; each flag overrides its default, and a wrong value is refused
/// with a reason.
#[test]
fn the_hibachi_command_line_takes_documented_defaults_and_refuses_with_a_reason() {
    let parsed = args::parse(strings(&[
        "--hibachi",
        "ETH/USDT-P",
        "--binance",
        "ETHUSDT",
    ]));
    let Ok(Parsed::Watch(opts)) = parsed else {
        panic!("{parsed:?}")
    };
    let defaults = HibachiOpts {
        symbol: "ETH/USDT-P".to_owned(),
        rest: "https://data-api.hibachi.xyz".to_owned(),
        ws: "wss://data-api.hibachi.xyz".to_owned(),
        timeout_ms: 5_000,
        depth: 15,
        granularity: None,
        retry_ms: 2_000,
        status_s: 60,
        auction_halt_s: 900,
    };
    assert_eq!(opts.hibachi.as_ref(), Some(&defaults));
    assert_eq!(opts.binance.as_ref().unwrap().symbol, "ETHUSDT");
    for (said, value) in [
        ("--hibachi-rest <URL>", "https://data-api.hibachi.xyz"),
        ("--hibachi-ws <URL>", "wss://data-api.hibachi.xyz"),
        ("--hibachi-timeout <MS>", "(default 5000)"),
        ("--hibachi-depth <N>", "(default 15"),
        ("--hibachi-retry <MS>", "milliseconds (default 2000)"),
        ("--hibachi-status <S>", "seconds (default 60)"),
        ("--hibachi-auction-halt <S>", "seconds (default 900"),
    ] {
        let at = USAGE
            .find(said)
            .unwrap_or_else(|| panic!("{said} undocumented"));
        let entry: String = USAGE[at..].lines().take(4).collect();
        assert!(
            entry.contains(value),
            "{said} does not state {value}: {entry}"
        );
    }
    // The defaults are a configuration Hibachi's factory accepts as it stands.
    use fbc_core::VenueFactory;
    let cfg = hibachi::config(&defaults);
    fbc_venue_hibachi::Hibachi.caps(&cfg).unwrap();
    for (key, value) in [
        ("hibachi.data_rest_url", "https://data-api.hibachi.xyz"),
        ("hibachi.rest_timeout", "5000ms"),
        ("hibachi.orderbook_depth", "15"),
        ("hibachi.data_ws_url", "wss://data-api.hibachi.xyz"),
        ("hibachi.orderbook_resubscribe", "2000ms"),
        ("hibachi.status_interval", "60s"),
        ("hibachi.auction_halt", "900s"),
    ] {
        assert_eq!(cfg.get(key), Some(value), "{key}");
    }

    let parsed = args::parse(strings(&[
        "--hibachi",
        "kPEPE/USDT-P",
        "--hibachi-rest",
        "http://127.0.0.1:8080",
        "--hibachi-ws",
        "ws://127.0.0.1:8081",
        "--hibachi-timeout",
        "750",
        "--hibachi-depth",
        "100",
        "--hibachi-granularity",
        "0.1",
        "--hibachi-retry",
        "250",
        "--hibachi-status",
        "5",
        "--hibachi-auction-halt",
        "30",
    ]));
    let Ok(Parsed::Watch(opts)) = parsed else {
        panic!("{parsed:?}")
    };
    assert_eq!(
        opts.hibachi,
        Some(HibachiOpts {
            symbol: "kPEPE/USDT-P".to_owned(),
            rest: "http://127.0.0.1:8080".to_owned(),
            ws: "ws://127.0.0.1:8081".to_owned(),
            timeout_ms: 750,
            depth: 100,
            granularity: Some(Decimal::new(1, 1)),
            retry_ms: 250,
            status_s: 5,
            auction_halt_s: 30,
        })
    );
    // Each value given is the one the venue's configuration carries.
    let cfg = hibachi::config(opts.hibachi.as_ref().unwrap());
    fbc_venue_hibachi::Hibachi.caps(&cfg).unwrap();
    for (key, value) in [
        ("hibachi.data_rest_url", "http://127.0.0.1:8080"),
        ("hibachi.rest_timeout", "750ms"),
        ("hibachi.orderbook_depth", "100"),
        ("hibachi.data_ws_url", "ws://127.0.0.1:8081"),
        ("hibachi.orderbook_resubscribe", "250ms"),
        ("hibachi.status_interval", "5s"),
        ("hibachi.auction_halt", "30s"),
    ] {
        assert_eq!(cfg.get(key), Some(value), "{key}");
    }
    // The other venues are unaffected and take no Hibachi flag.
    assert_eq!(
        (opts.paradex.is_none(), opts.binance.is_none()),
        (true, true)
    );

    for (args, why) in [
        (&["--hibachi"][..], "--hibachi needs a value"),
        (
            &["--hibachi", "ETH_USDT-P"][..],
            "--hibachi ETH_USDT-P: not a market as the venue spells it",
        ),
        (
            &["--hibachi", "ETH/USDT-P "][..],
            "not a market as the venue spells it",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-depth", "0"][..],
            "--hibachi-depth 0: not a whole number from 1 to 100",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-depth", "101"][..],
            "not a whole number from 1 to 100",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-timeout", "0"][..],
            "--hibachi-timeout 0: not a positive whole number of milliseconds",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-retry", "2s"][..],
            "--hibachi-retry 2s: not a positive whole number of milliseconds",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-granularity", "-0.1"][..],
            "not a positive decimal",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-status", "0"][..],
            "--hibachi-status 0: not a positive whole number of seconds",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-auction-halt", "15m"][..],
            "--hibachi-auction-halt 15m: not a positive whole number of seconds",
        ),
        (
            &["--hibachi", "X/USDT-P", "--hibachi-ws", "--top", "2"][..],
            "--hibachi-ws needs a value",
        ),
    ] {
        let err = args::parse(strings(args)).unwrap_err();
        assert!(err.contains(why), "{args:?}: {err}");
    }
    let err = args::parse(strings(&["--top", "2"])).unwrap_err();
    assert!(err.contains("--hibachi <SYMBOL>"), "{err}");
}

/// The contract read: ETH/USDT-P on its own tick, step and quote grid, as a consumer's
/// discovery reads it; `--hibachi-granularity` in place of the quote grid, in its shortest
/// spelling, as no quote grid at the tick itself, refused when it is not a whole number of ticks
/// and never overflowing however coarse; and a contract Hibachi does not list refused by name.
#[tokio::test]
async fn md_watch_reads_the_hibachi_contract_through_the_venues_discovery() {
    let stub = StubServer::start(WsScript::new(Vec::new()), exchange_info())
        .await
        .unwrap();
    let read = |symbol: &str, more: &[&str]| {
        let opts = hibachi_args(&stub, symbol, more).hibachi.unwrap();
        async move {
            let factory = hibachi::Counting::leaked();
            let cfg = hibachi::config(&opts);
            use fbc_core::VenueFactory;
            let caps = factory.caps(&cfg).unwrap();
            let reserve = fbc_runtime::SafetyReserve::percent(10).unwrap();
            let limiter = fbc_runtime::RateLimiter::new(&caps.limits, reserve).unwrap();
            let connector = fbc_runtime::Connector::new(ProxyConfig::Direct);
            let inst = fbc_core::InstrumentId::new(3);
            hibachi::read_contract(factory, &cfg, &opts, inst, &connector, &limiter).await
        }
    };
    let (eth, listed) = read("ETH/USDT-P", &[]).await.unwrap();
    assert_eq!(eth.venue_symbol.as_wire(), "ETH/USDT-P");
    assert_eq!(eth.price_grid.finest(), Decimal::new(1, 6));
    assert_eq!(eth.size_step.get(), Decimal::new(1, 9));
    assert_eq!(eth.quote_grid, Some(Decimal::new(1, 2)));
    assert_eq!(listed.status, fbc_core::TradingStatus::Trading);
    assert!(listed.sessions.is_none());

    let (eth, _) = read("ETH/USDT-P", &["--hibachi-granularity", "0.1"])
        .await
        .unwrap();
    assert_eq!(eth.quote_grid, Some(Decimal::new(1, 1)));
    // Typed with a trailing zero, it is asked for in its shortest spelling: the subscription
    // writes the quote grid's text (on the wire in the counting factory's test below).
    let (eth, _) = read("ETH/USDT-P", &["--hibachi-granularity", "0.10"])
        .await
        .unwrap();
    assert_eq!(
        eth.quote_grid.map(|g| g.to_string()),
        Some("0.1".to_owned())
    );
    // The tick itself is the book on the acceptance grid: no quote grid, as fbc-core defines it.
    let (eth, _) = read("ETH/USDT-P", &["--hibachi-granularity", "0.000001"])
        .await
        .unwrap();
    assert_eq!(eth.quote_grid, None);
    let err = read("ETH/USDT-P", &["--hibachi-granularity", "0.0000005"])
        .await
        .unwrap_err();
    assert!(
        err.contains("not a whole number of ETH/USDT-P's tick"),
        "{err}"
    );
    // A granularity whose count of ticks is past what a decimal holds is checked without
    // overflow (a division here once panicked): a whole number of ticks all the same, so it is
    // taken as given.
    let coarsest = "79228162514264337593543950335";
    let (eth, _) = read("ETH/USDT-P", &["--hibachi-granularity", coarsest])
        .await
        .unwrap();
    assert_eq!(
        eth.quote_grid.map(|g| g.to_string()),
        Some(coarsest.to_owned())
    );
    let err = read("BTC/USDT-P", &[]).await.unwrap_err();
    assert_eq!(err, "BTC/USDT-P: not a contract Hibachi lists");
    // Every read was the stub's.
    assert_eq!(stub.http_requests().len(), 7);
    assert!(
        stub.http_requests()
            .iter()
            .all(|r| r == "GET /market/exchange-info")
    );
}

/// md_watch's contract read makes only a bare public GET, each charged to Hibachi's declared
/// rate limit before it is sent: the plan's one request is charged once and answered by the
/// stub; one the limit has no room for is refused, and so is one with another method, a header
/// or a body, none of them charged or sent. That the limiter charged is the one the sessions are
/// then given is `watch.rs`'s
/// `a_hibachi_contracts_read_is_charged_to_the_limiter_its_sessions_are_given`.
#[tokio::test]
async fn md_watch_sends_the_contract_read_as_a_bare_get_charged_to_the_rate_limit() {
    use fbc_core::{Effect, Header, HttpMethod, HttpRequest, VenueFactory, WireSlice};
    use fbc_runtime::{BucketKey, Connector, RateLimiter, SafetyReserve};
    use fbc_venue_hibachi::Hibachi;
    use tokio::time::Instant;

    let stub = StubServer::start(WsScript::new(Vec::new()), exchange_info())
        .await
        .unwrap();
    let opts = hibachi_args(&stub, "ETH/USDT-P", &[]).hibachi.unwrap();
    let cfg = hibachi::config(&opts);
    let caps = Hibachi.caps(&cfg).unwrap();
    // Hibachi declares one limit: 350 REST requests per 10 s per IP, its one shared bucket.
    let limiter = || RateLimiter::new(&caps.limits, SafetyReserve::percent(10).unwrap()).unwrap();
    let used = |limiter: &RateLimiter| limiter.used(Instant::now(), 0, BucketKey::Shared);
    let connector = Connector::new(ProxyConfig::Direct);
    let get = Hibachi.discover(&cfg).unwrap().requests().to_vec();

    let fresh = limiter();
    let answers = hibachi::fetch(&get, &connector, &fresh).await.unwrap();
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].1, 200);
    assert_eq!(used(&fresh), 1);
    assert_eq!(stub.http_requests(), ["GET /market/exchange-info"]);

    // Normal traffic stops at the safety reserve, 35 short of the 350.
    let full = limiter();
    let charge = fbc_runtime::Request::of(&get[0]).unwrap();
    let conn = fbc_core::ConnKey {
        conn: u16::MAX,
        epoch: 0,
    };
    let admitted = (0..400)
        .take_while(|_| full.charge(Instant::now(), conn, &[charge]).is_ok())
        .count();
    assert_eq!(admitted, 315);
    let err = hibachi::fetch(&get, &connector, &full).await.unwrap_err();
    assert_eq!(
        err,
        "GET /market/exchange-info: refused by Hibachi's declared rate limit"
    );
    assert_eq!(used(&full), 315);

    let altered = |change: fn(&mut HttpRequest)| {
        let mut effects = get.clone();
        let Effect::Http { req, .. } = &mut effects[0] else {
            panic!("not HTTP: {effects:?}")
        };
        change(req);
        effects
    };
    let post: fn(&mut HttpRequest) = |req| req.method = HttpMethod::Post;
    let header: fn(&mut HttpRequest) = |req| {
        req.headers.push(Header {
            name: "accept",
            value: "application/json".to_owned(),
            redact: false,
        })
    };
    let body: fn(&mut HttpRequest) = |req| req.body = WireSlice::plain(b"{}".to_vec());
    for (what, change) in [("a POST", post), ("a header", header), ("a body", body)] {
        let fresh = limiter();
        let err = hibachi::fetch(&altered(change), &connector, &fresh)
            .await
            .unwrap_err();
        assert_eq!(
            err, "GET /market/exchange-info: the plan asks for a request md_watch does not make",
            "{what}"
        );
        assert_eq!(used(&fresh), 0, "{what}");
    }
    // Only the one read as planned reached the stub.
    assert_eq!(stub.http_requests(), ["GET /market/exchange-info"]);
}

/// The ticket's fact that a closed FX market or a maintenance window is reported closed, not as
/// a disconnect: the `MARKET` line says `closed` for a contract the read lists as anything but
/// trading (GBP/USDT-P, `PAUSED`) and, for one listed trading, while its session calendar closes
/// it (EUR/USDT-P, an FX pair, over the New York weekend), and `open` otherwise; ETH/USDT-P has
/// no calendar and is open at every instant.
#[tokio::test]
async fn a_hibachi_contract_is_reported_closed_by_its_status_or_its_calendar() {
    let stub = StubServer::start(WsScript::new(Vec::new()), exchange_info())
        .await
        .unwrap();
    let listed = |symbol: &'static str| {
        let opts = hibachi_args(&stub, symbol, &[]).hibachi.unwrap();
        async move {
            let factory = hibachi::Counting::leaked();
            let cfg = hibachi::config(&opts);
            use fbc_core::VenueFactory;
            let caps = factory.caps(&cfg).unwrap();
            let reserve = fbc_runtime::SafetyReserve::percent(10).unwrap();
            let limiter = fbc_runtime::RateLimiter::new(&caps.limits, reserve).unwrap();
            let connector = fbc_runtime::Connector::new(ProxyConfig::Direct);
            let inst = fbc_core::InstrumentId::new(3);
            let read = hibachi::read_contract(factory, &cfg, &opts, inst, &connector, &limiter);
            read.await.unwrap().1
        }
    };
    // 2026-10-24, a Saturday, 12:00 UTC; 2026-10-21, a Wednesday, 12:00 UTC.
    let saturday = WallNs(1_792_843_200 * 1_000_000_000);
    let wednesday = WallNs(1_792_584_000 * 1_000_000_000);
    let (eur, gbp, eth) = (
        listed("EUR/USDT-P").await,
        listed("GBP/USDT-P").await,
        listed("ETH/USDT-P").await,
    );
    assert_eq!(eur.state(saturday), "closed by its session calendar");
    assert_eq!(eur.state(wednesday), "open");
    assert_eq!(gbp.state(wednesday), "closed: listed Halted");
    assert_eq!(gbp.state(saturday), "closed: listed Halted");
    assert_eq!(eth.state(saturday), "open");
    assert_eq!(eth.state(wednesday), "open");
    // Reviewer B RB213-2: the listed status is the start read's, so a contract the read finds
    // PAUSED reads closed at every later instant too (GBP above, on the Wednesday); the usage
    // text and the README say so, and (FBC-1632) that the status poll's STATUS lines show each
    // later change, which MARKET does not follow.
    let flat = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
    let usage = flat(USAGE);
    for said in [
        "its listed status is the start read's",
        "reads closed here all run, even once Hibachi reopens it",
        "the STATUS lines show each change the status poll reads after it",
    ] {
        assert!(usage.contains(said), "--help does not say {said}");
    }
    let readme = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let readme = flat(&fs::read_to_string(readme).unwrap());
    for said in [
        "holds there for the whole run in both directions",
        "closed: listed Halted` for the whole run, even after Hibachi reopens it",
        "a halt Hibachi lists later is seen there",
        "`MARKET` does not follow it",
    ] {
        assert!(readme.contains(said), "the README does not say {said}");
    }
}

/// The done line's command is in the usage text and in the crate's README, beside the README's
/// account of how the owner's first live run settles whether Hibachi honours the orderbook
/// subscription's granularity and depth.
#[test]
fn the_usage_text_and_the_readme_show_the_hibachi_command() {
    let command = "cargo run -p fbc-examples --example md_watch -- --hibachi ETH/USDT-P --binance \
                   ETHUSDT";
    assert!(USAGE.contains(command), "--help does not show {command}");
    let readme = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let readme = fs::read_to_string(readme).unwrap();
    assert!(
        readme.contains(command),
        "the README does not show {command}"
    );
    for said in [
        "param_mismatches",
        "--hibachi-depth",
        "--hibachi-granularity",
    ] {
        assert!(readme.contains(said), "the README does not say {said}");
    }
    // Reviewer B RB213-1: the README tells four outcomes apart, each by a signature of its own.
    let outcomes = readme_outcomes();
    let labels: Vec<&str> = outcomes.iter().map(|(label, _)| label.as_str()).collect();
    assert_eq!(
        labels,
        [
            "Not honoured",
            "Not answered",
            "Honoured, or the venue's defaults happen to be what was asked",
            "Taken, but the book fails another check",
        ]
    );
}

/// Nothing in the repository runs md_watch against a venue: no CI workflow or script names it
/// (CI only builds it, as every example, through the workspace's build and tests), and every run
/// in this file is against a stub on 127.0.0.1, its Hibachi runs asserting that the contract read
/// and the connection reached the stub.
#[test]
fn no_ci_workflow_or_script_runs_md_watch() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    let mut dirs: Vec<PathBuf> = [".github", "scripts", "ci"]
        .iter()
        .map(|d| root.join(d))
        .filter(|d| d.is_dir())
        .collect();
    assert!(
        !dirs.is_empty(),
        "no CI or script directory under {}",
        root.display()
    );
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                files.push(path);
            }
        }
    }
    assert!(files.iter().any(|f| f.ends_with("check.sh")), "{files:?}");
    for file in files {
        let Ok(text) = fs::read_to_string(&file) else {
            continue;
        };
        assert!(
            !text.contains("md_watch"),
            "{} names md_watch: nothing in the repository runs it against a venue",
            file.display()
        );
    }
}

/// md_watch's `hibachi::Counting` is `Hibachi` in every call but `md_codec`, whose codec is
/// `Hibachi`'s own with its counts copied out after each call: the runtime's sessions see the
/// same venue, and md_watch can print what the codec counted.
#[test]
fn md_watchs_counting_factory_is_hibachis_own_with_the_codecs_counts_copied_out() {
    use fbc_core::{
        Effect, Effects, Feed, HttpTag, Inbound, InboundSpans, RawFrame, Secrets, SpecTable,
        Subscription, VenueFactory,
    };
    use fbc_venue_hibachi::{BOOK_ORDERBOOK, Hibachi};
    let factory = hibachi::Counting::leaked();
    let opts = HibachiOpts {
        symbol: "ETH/USDT-P".to_owned(),
        rest: "https://data-api.hibachi.xyz".to_owned(),
        ws: "wss://data-api.hibachi.xyz".to_owned(),
        timeout_ms: 5_000,
        depth: 3,
        granularity: None,
        retry_ms: 2_000,
        status_s: 60,
        auction_halt_s: 900,
    };
    let cfg = hibachi::config(&opts);
    assert_eq!(factory.id(), Hibachi.id());
    assert_eq!(factory.config_schema(), Hibachi.config_schema());
    assert_eq!(factory.caps(&cfg), Hibachi.caps(&cfg));
    assert_eq!(
        factory.parse_fbc_common_symbol("ETH/USDT"),
        Hibachi.parse_fbc_common_symbol("ETH/USDT")
    );
    assert_eq!(factory.plan_exec(&cfg), Hibachi.plan_exec(&cfg));
    assert!(factory.exec_codec(&cfg, Secrets::new()).is_none());
    assert!(factory.test_connection(&cfg, Secrets::new()).is_none());
    let (ours, theirs) = (
        factory.discover(&cfg).unwrap(),
        Hibachi.discover(&cfg).unwrap(),
    );
    assert_eq!(ours.requests(), theirs.requests());

    // No subscription plans nothing, as Hibachi's own; a codec's counts reach the tally, and a
    // new connection's codec starts it from none.
    let none = std::collections::BTreeSet::new();
    let specs = SpecTable::new();
    assert_eq!(
        factory.plan_md(&cfg, &specs, &none),
        Hibachi.plan_md(&cfg, &specs, &none)
    );
    let ep = fbc_core::EndpointPlan {
        stream: fbc_core::StreamId(0),
        transport: fbc_core::MdTransport::Socket {
            url: fbc_core::WireUrl::plain("wss://data-api.hibachi.xyz/ws/market"),
        },
        subs: Vec::new(),
    };
    let mut codec = factory.md_codec(&cfg, &ep);
    let mut fx = Effects::new();
    codec.on_open(&mut fx);
    assert!(fx.take().is_empty());
    assert!(codec.keepalive().is_none());
    let ack_text = r#"{"id":1,"status":200}"#;
    let inbound = Inbound::Frame(RawFrame::Text(ack_text));
    assert_eq!(codec.redact_inbound(inbound), InboundSpans::NONE);
    let caps = Hibachi.caps(&cfg).unwrap();
    struct Nothing;
    impl fbc_core::MdSink for Nothing {
        fn push(&mut self, _: fbc_core::VenueMeta, ev: fbc_core::MdEvent) {
            panic!("pushed {ev:?}")
        }
    }
    let decoded = fbc_core::dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Text(ack_text),
            scope,
            &specs,
            &mut Nothing,
            &mut fx,
        )
    });
    decoded.unwrap();
    assert_eq!(factory.tally.get().acks, 1);
    // The codec asks for no HTTP request, so it refuses an answer to one, counting nothing.
    let answer = fbc_core::HttpResponse {
        status: 200,
        headers: &[],
        body: b"{}",
    };
    let refused = fbc_core::dispatch_market_data(&caps, |scope| {
        codec.on_http(HttpTag(9), Ok(answer), scope, &specs, &mut Nothing, &mut fx)
    });
    assert!(refused.is_err());
    // A timer no book armed changes nothing.
    codec.on_timer(
        fbc_core::TimerTag(9),
        fbc_core::MonoNs(0),
        WallNs(0),
        &mut Nothing,
        &mut fx,
    );
    // A subscription of an instrument the spec table lacks is refused, sending nothing.
    let sub = Subscription {
        inst: fbc_core::InstrumentId::new(3),
        feed: Feed::Book(BOOK_ORDERBOOK),
    };
    assert!(codec.subscribe(&[sub], &[], &specs, &mut fx).is_err());
    assert!(fx.take().is_empty());
    assert_eq!(factory.tally.get().acks, 1);
    // FBC-1632 (decision 0130): the status poll, which Hibachi plans alone for no subscription,
    // gets Hibachi's own codec, not a counted socket codec: building it and opening it, which
    // asks for its first read, leave the socket's tally as it was.
    let polls = factory.plan_md(&cfg, &specs, &none).unwrap();
    let [poll] = polls.as_slice() else {
        panic!("not the status poll alone: {polls:?}")
    };
    assert!(
        matches!(poll.transport, fbc_core::MdTransport::Poll { .. }),
        "{poll:?}"
    );
    let mut status = factory.md_codec(&cfg, poll);
    status.on_open(&mut fx);
    assert!(!fx.take().is_empty(), "the status poll asked for no read");
    assert_eq!(factory.tally.get().acks, 1);
    let _next = factory.md_codec(&cfg, &ep);
    assert_eq!(factory.tally.get(), Default::default());
    assert_eq!(hibachi::counts_text(&factory.tally.take()), "none");

    // What the codec counts on its timer reaches the tally at that call, with no frame after
    // it: ETH/USDT-P's orderbook subscribed, at `--hibachi-granularity 0.10` written as 0.1, and
    // no Snapshot within the wait it arms, so its timer reports the gap and resubscribes.
    let mut opts = opts.clone();
    opts.granularity = Some("0.10".parse().unwrap());
    let (spec, _) = eth_contract(&opts);
    let mut specs = SpecTable::new();
    specs.insert(spec);
    let mut codec = factory.md_codec(&cfg, &ep);
    let book = Subscription {
        inst: fbc_core::InstrumentId::new(3),
        feed: Feed::Book(BOOK_ORDERBOOK),
    };
    codec.subscribe(&[book], &[], &specs, &mut fx).unwrap();
    let effects = fx.take();
    let [Effect::Send { frame, .. }, Effect::Timer { tag, .. }] = effects.as_slice() else {
        panic!("not a subscribe and its wait: {effects:?}")
    };
    let sent: serde_json::Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(
        sent["parameters"]["subscriptions"][0]["granularity"], "0.1",
        "{sent}"
    );
    struct Kept(Vec<fbc_core::MdEvent>);
    impl fbc_core::MdSink for Kept {
        fn push(&mut self, _: fbc_core::VenueMeta, ev: fbc_core::MdEvent) {
            self.0.push(ev);
        }
    }
    let mut kept = Kept(Vec::new());
    codec.on_timer(*tag, fbc_core::MonoNs(0), WallNs(0), &mut kept, &mut fx);
    assert_eq!(kept.0.len(), 1, "{:?}", kept.0);
    assert_eq!(factory.tally.get().resubscribes, 1);
}

/// ETH/USDT-P's spec as md_watch reads it from the hand-built `GET /market/exchange-info`
/// answer, with no socket: the discovery plan's one request answered with the fixture.
fn eth_contract(opts: &HibachiOpts) -> (fbc_core::InstrumentSpec, hibachi::Listed) {
    use fbc_core::{Effect, VenueFactory};
    let cfg = hibachi::config(opts);
    let caps = fbc_venue_hibachi::Hibachi.caps(&cfg).unwrap();
    let plan = fbc_venue_hibachi::Hibachi.discover(&cfg).unwrap();
    let tag = match plan.requests() {
        [Effect::Http { tag, .. }] => *tag,
        other => panic!("not one request: {other:?}"),
    };
    let body = fs::read(fixture_path("hibachi/rest/exchange_info.json")).unwrap();
    let answers = [(tag, 200, body.into())];
    let inst = fbc_core::InstrumentId::new(3);
    hibachi::contract(plan, &caps, &answers, opts, inst).unwrap()
}

/// The contract read fails, saying why, when the REST origin answers with an error status or
/// does not answer at all; md_watch then connects nothing.
#[tokio::test]
async fn md_watch_refuses_a_hibachi_contract_read_that_fails() {
    let stub = StubServer::start(
        WsScript::new(Vec::new()),
        HttpRoutes::from([(
            "/market/exchange-info".to_owned(),
            HttpReply {
                status: 503,
                body: b"{}".to_vec(),
            },
        )]),
    )
    .await
    .unwrap();
    let opts = hibachi_args(&stub, "ETH/USDT-P", &[]);
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let err = watch::run(&opts, buf.clone(), false, std::future::pending())
        .await
        .unwrap_err();
    assert!(
        err.starts_with("hibachi: GET /market/exchange-info: ")
            && err.contains("answered with status 503"),
        "{err}"
    );
    assert!(buf.borrow().is_empty(), "md_watch printed before the read");
    assert!(stub.connections().is_empty());

    // An origin nothing listens on: the port of a listener just closed.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let rest = format!("http://127.0.0.1:{port}");
    let mut opts = opts;
    opts.hibachi.as_mut().unwrap().rest = rest;
    let err = watch::run(&opts, buf.clone(), false, std::future::pending())
        .await
        .unwrap_err();
    assert!(
        err.starts_with("hibachi: GET /market/exchange-info: "),
        "{err}"
    );
    assert!(stub.connections().is_empty());
}

/// A SOCKS5 proxy (RFC 1928, no authentication) on a 127.0.0.1 ephemeral port that relays each
/// CONNECT to the address it names (an IPv4 address, or `localhost`) and records each target as
/// `<host>:<port>`, in the order the requests arrived. A request it cannot read is recorded as
/// `refused` and closed.
async fn socks5_relay() -> (u16, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use std::net::Ipv4Addr;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn target(client: &mut TcpStream) -> Option<(String, u16)> {
        let mut greeting = [0u8; 2];
        client.read_exact(&mut greeting).await.ok()?;
        let mut methods = vec![0u8; usize::from(greeting[1])];
        client.read_exact(&mut methods).await.ok()?;
        (greeting[0] == 5 && methods.contains(&0)).then_some(())?;
        client.write_all(&[5, 0]).await.ok()?;
        let mut head = [0u8; 4];
        client.read_exact(&mut head).await.ok()?;
        (head[..3] == [5, 1, 0]).then_some(())?;
        let host = match head[3] {
            1 => {
                let mut ip = [0u8; 4];
                client.read_exact(&mut ip).await.ok()?;
                Ipv4Addr::from(ip).to_string()
            }
            3 => {
                let mut name = vec![0u8; usize::from(client.read_u8().await.ok()?)];
                client.read_exact(&mut name).await.ok()?;
                String::from_utf8(name).ok()?
            }
            _ => return None,
        };
        Some((host, client.read_u16().await.ok()?))
    }

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let targets = Arc::new(Mutex::new(Vec::new()));
    let log = targets.clone();
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let Some((host, port)) = target(&mut client).await else {
                    log.lock().unwrap().push("refused".to_owned());
                    return;
                };
                log.lock().unwrap().push(format!("{host}:{port}"));
                let ip = if host == "localhost" {
                    Ipv4Addr::LOCALHOST
                } else {
                    host.parse().unwrap_or(Ipv4Addr::UNSPECIFIED)
                };
                let Ok(mut upstream) = TcpStream::connect((ip, port)).await else {
                    let _ = client.write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0]).await;
                    return;
                };
                let ok = [5, 0, 0, 1, 127, 0, 0, 1, 0, 0];
                if client.write_all(&ok).await.is_ok() {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                }
            });
        }
    });
    (port, targets)
}

/// Reviewer B RB213-4: `--socks5` applies to Hibachi's contract read as to its WebSocket. The
/// read is md_watch's one HTTP request made outside the runtime's sessions, through the run's
/// connector; with `--socks5` pointed at a relay, the relay is asked to CONNECT first to the
/// stub's HTTP port (the read), then to its WebSocket port and, for the status poll's first read
/// (FBC-1632), to its HTTP port again, those two in either order; md_watch prints the book the
/// stub's Snapshot carries and the status the poll read, so all three went through it and nothing
/// went around it.
#[tokio::test]
async fn md_watch_sends_the_hibachi_contract_read_and_its_websocket_through_the_socks5_proxy() {
    let mut steps = vec![Step::Accept];
    steps.extend((0..3).map(|_| Step::Read { conn: 0 }));
    steps.push(Step::Push {
        conn: 0,
        frame: hibachi_frame("eth_s0"),
    });
    let stub = StubServer::start(WsScript::new(steps), exchange_info())
        .await
        .unwrap();
    let (proxy, targets) = socks5_relay().await;
    let proxy = format!("127.0.0.1:{proxy}");
    let opts = hibachi_args(&stub, "ETH/USDT-P", &["--socks5", &proxy]);
    assert!(matches!(opts.proxy, ProxyConfig::Socks5 { .. }));
    let book = "BOOK hibachi ETH/USDT-P orderbook bid 2000 x 1.25 ask 2000.1 x 1.5";
    let lines = watch_until(&opts, false, |t| {
        t.contains(book) && t.contains(ETH_TRADING)
    })
    .await;
    stub.finished().await.unwrap();

    assert_eq!(lines.iter().filter(|l| l.starts_with(book)).count(), 1);
    assert_eq!(socket_lines(&lines).len() + 2, lines.len(), "{lines:#?}");
    let http = stub.http_url("");
    let ws = stub.ws_url("");
    let (http, ws) = (
        http.strip_prefix("http://").unwrap(),
        ws.strip_prefix("ws://").unwrap(),
    );
    let relayed = targets.lock().unwrap().clone();
    assert_eq!(
        relayed.first().map(String::as_str),
        Some(http),
        "{relayed:?}"
    );
    let mut after = relayed[1..].to_vec();
    after.sort_unstable();
    let mut both = vec![http.to_owned(), ws.to_owned()];
    both.sort_unstable();
    assert_eq!(after, both, "{relayed:?}");
    assert_eq!(stub.http_requests(), ["GET /market/exchange-info"; 2]);
    assert_eq!(stub.connections().len(), 1);
}
