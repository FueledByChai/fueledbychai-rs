//! FBC-u4so's done line: md_watch's session wiring, run against the conformance kit's stub
//! server on 127.0.0.1, prints the touch, book, trade and reconnect lines both venues' frames
//! call for, and its `--help` documents every argument.
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
#[path = "../examples/md_watch/watch.rs"]
mod watch;

use std::cell::RefCell;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use args::{Market, Options, Parsed, USAGE};
use fbc_conformance::{Frame, HttpReply, HttpRoutes, Step, StubServer, WsScript};
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
        binance: None,
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
        (
            &["--paradex", "BTC-USD-PERP "][..],
            "not a market as the venue spells it",
        ),
        (
            &["--paradex", "BTC_USD_PERP"][..],
            "not a market as the venue spells it",
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

/// Standard output as `| head` leaves it: the first line is taken, then the reader is gone and
/// every write fails with a broken pipe.
struct ClosedAfterFirstLine {
    taken: Rc<RefCell<Vec<u8>>>,
}

impl std::io::Write for ClosedAfterFirstLine {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut taken = self.taken.borrow_mut();
        if taken.contains(&b'\n') {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        taken.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// RB98-4: `md_watch ... | head` ends. Run with no stop of its own, md_watch prints its header,
/// and the first line the venue's frames call for fails to write: md_watch stops, closing the
/// connection, instead of watching forever with nowhere to print.
#[tokio::test]
async fn md_watch_stops_once_standard_output_is_closed() {
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
    let out: watch::Out = Rc::new(RefCell::new(ClosedAfterFirstLine {
        taken: taken.clone(),
    }));
    let run = watch::run(&opts, out, false, std::future::pending());
    let Ok(ran) = tokio::time::timeout(Duration::from_secs(60), run).await else {
        panic!("md_watch kept running for 60 s after its standard output closed")
    };
    ran.unwrap();
    paradex.finished().await.unwrap();
    assert_eq!(
        String::from_utf8(taken.borrow().clone()).unwrap(),
        "md_watch: paradex BTC-USD-PERP (bbo, trades, deltas book); market data only, no order \
         is ever sent\n"
    );
    assert_eq!(paradex.connections().len(), 1);
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
