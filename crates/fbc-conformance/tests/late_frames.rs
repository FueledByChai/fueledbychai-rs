//! FBC-pn85's done line: the order-entry checks wait for the stub to have sent, and the session
//! to have read, every frame of a step by an event (a barrier ending every reply, the stub's
//! script position), bounded by a wall-clock watchdog, never by a count of scheduler yields.
//!
//! The proof is a toy variant answering a placement as Paradex does, its order event a second
//! frame after the reply, run through a loopback proxy that holds that second frame back by the
//! wall clock, as a loaded host delivers it late (PR #145's CI run 37913798868: `amend_ack`
//! read the placement's late `Open` as contradicting the amend). `amend_ack` passes it. Around
//! it: an opening that answers nothing fails at the watchdog, set short for the test, and the
//! failure paths the stub or the session report by an event (a mute opening, a refusing reply,
//! a request not sent) fail within 5 s each.

mod toy_setup;

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use fbc_conformance::suite::{self, Failure, Replier, Setup, Subject, Verdict};
use fbc_conformance::toy::{self, ToyFactory};
use fbc_conformance::{Frame, Responder, StubServer};
use fbc_core::VenueConfig;
use toy_setup::{FIXTURES, assumed, order_entry};

/// How long, by the wall clock, the proxy holds a placement's order event back.
const LATE: Duration = Duration::from_millis(300);
/// The most a check reported failing by an event may take, by the wall clock.
const FAST: Duration = Duration::from_secs(5);

/// The value of `key` in a `kind|key=value|...` record.
fn field<'a>(record: &'a str, key: &str) -> Option<&'a str> {
    let kv = record
        .split('|')
        .skip(1)
        .filter_map(|kv| kv.split_once('='));
    kv.into_iter().find_map(|(k, v)| (k == key).then_some(v))
}

/// The toy's setup whose stub answers an accepted placement as Paradex does, its reply then
/// the order's `Open` event as a second frame, through a proxy that holds that event back by
/// [`LATE`] of wall-clock time.
fn late_open() -> Setup {
    let mut stub = order_entry();
    let inner = stub.reply.clone();
    stub.reply = Replier::new(move |frame, answers| {
        let mut frames = inner.reply(frame, answers)?;
        let Frame::Text(request) = frame else {
            return Ok(frames);
        };
        if !request.starts_with("place|") {
            return Ok(frames);
        }
        let vid = frames.iter().find_map(|f| match f {
            Frame::Text(t) if t.contains("res=ok") => field(t, "vid").map(str::to_owned),
            _ => None,
        });
        if let Some(vid) = vid {
            let f = |key| field(request, key).unwrap_or_default();
            frames.push(Frame::Text(format!(
                "order|cid={}|vid={vid}|sym={}|side={}|st=open|cum=0|px={}|qty={}|po={}|ro={}|seq=0",
                f("cid"),
                f("sym"),
                f("side"),
                f("px"),
                f("qty"),
                f("po"),
                f("ro"),
            )));
        }
        Ok(frames)
    });
    stub.point = point_through_late_proxy;
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// Points the toy's order entry at a proxy in front of the stub's socket that forwards every
/// byte at once, but for each `Open` order event the stub sends, which it forwards [`LATE`]
/// after it read it, in order: what follows that frame waits behind it.
fn point_through_late_proxy(cfg: &mut VenueConfig, stub: &StubServer) {
    let url = stub.ws_url("");
    let upstream: SocketAddr = url.trim_start_matches("ws://").parse().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let at = listener.local_addr().unwrap();
    thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { return };
            let Ok(server) = TcpStream::connect(upstream) else {
                return;
            };
            proxy(client, server);
        }
    });
    cfg.insert(toy::EXEC_URL_KEY, &format!("ws://{at}/exec"));
}

/// Forwards `client`'s bytes to `server` as they come, and `server`'s to `client` frame by
/// frame, holding each `Open` order event back by [`LATE`].
fn proxy(client: TcpStream, server: TcpStream) {
    let (mut up_from, mut up_to) = (client.try_clone().unwrap(), server.try_clone().unwrap());
    thread::spawn(move || {
        let _ = io::copy(&mut up_from, &mut up_to);
        let _ = up_to.shutdown(Shutdown::Write);
    });
    thread::spawn(move || {
        let mut down_to = client;
        let mut from = BufReader::new(server);
        let _ = forward_down(&mut from, &mut down_to);
        let _ = down_to.shutdown(Shutdown::Both);
    });
}

fn forward_down(from: &mut BufReader<TcpStream>, to: &mut TcpStream) -> io::Result<()> {
    // The upgrade's response head, as it is.
    let mut line = Vec::new();
    loop {
        line.clear();
        if from.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        to.write_all(&line)?;
        if line == b"\r\n" {
            break;
        }
    }
    // Then each frame whole: the stub's are unmasked.
    loop {
        let mut head = [0u8; 2];
        from.read_exact(&mut head)?;
        let mut raw = head.to_vec();
        let len = match head[1] & 0x7f {
            126 => {
                let mut ext = [0u8; 2];
                from.read_exact(&mut ext)?;
                raw.extend_from_slice(&ext);
                u64::from(u16::from_be_bytes(ext))
            }
            127 => {
                let mut ext = [0u8; 8];
                from.read_exact(&mut ext)?;
                raw.extend_from_slice(&ext);
                u64::from_be_bytes(ext)
            }
            n => u64::from(n),
        };
        let mut payload = vec![0u8; usize::try_from(len).unwrap()];
        from.read_exact(&mut payload)?;
        let text = head[0] & 0x0f == 1;
        let open = text && payload.starts_with(b"order|") && contains(&payload, b"|st=open|");
        if open {
            thread::sleep(LATE);
        }
        raw.extend_from_slice(&payload);
        to.write_all(&raw)?;
    }
}

fn contains(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|w| w == needle)
}

/// The toy's setup whose opening reads each frame the session writes and answers none: the
/// epoch never takes places, and the stub's script waits on, failing nothing.
fn silent_opening() -> Setup {
    let mut stub = order_entry();
    let n = stub.opening.len();
    stub.opening = (0..n).map(|_| Responder::new(|_| Ok(Vec::new()))).collect();
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// The toy's setup whose opening answers nothing: its first reply reads the authentication,
/// which it refuses, so the stub's script stops there.
fn mute() -> Setup {
    let mut stub = order_entry();
    stub.opening.clear();
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

/// The toy's setup whose replies refuse every request.
fn refusing() -> Setup {
    let mut stub = order_entry();
    stub.reply = Replier::new(|_, _| Err("no request answered here".into()));
    Setup {
        order_entry: Some(stub),
        ..assumed()
    }
}

fn subject(setup: fn() -> Setup) -> Subject<'static> {
    Subject::new(&ToyFactory, FIXTURES, setup).unwrap()
}

fn failed(outcome: Result<Verdict, Failure>) -> Failure {
    outcome.expect_err("the check failed")
}

/// Whether a breach of `capability` says `what`.
fn says(failure: &Failure, capability: &str, what: &str) -> bool {
    let found = failure.breaches.iter();
    let mut found = found.filter(|b| b.capability == capability);
    found.any(|b| b.what.contains(what))
}

#[test]
fn amend_ack_passes_a_venue_whose_placement_order_event_reaches_the_session_late() {
    let passed = suite::amend_ack(&subject(late_open));
    assert!(matches!(passed, Ok(Verdict::Passed { .. })), "{passed:?}");
}

#[test]
fn an_opening_that_answers_nothing_fails_at_the_watchdog() {
    let started = Instant::now();
    let outcome = suite::with_watchdog(Duration::from_millis(500), || {
        suite::unknown_on_timeout(&subject(silent_opening))
    });
    let failure = failed(outcome);
    assert!(failure.names("OrderEntryStub.opening"), "{failure}");
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(500), "{took:?}");
    assert!(took < FAST, "{took:?}");
}

#[test]
fn the_failures_the_stub_or_the_session_report_end_within_five_seconds() {
    let started = Instant::now();
    let failure = failed(suite::unknown_on_timeout(&subject(mute)));
    assert!(failure.names("OrderEntryStub.opening"), "{failure}");
    assert!(
        started.elapsed() < FAST,
        "mute opening: {:?}",
        started.elapsed()
    );
    // amend_ack waits for the placement's reply on its own (`Ctx::replied`).
    for check in [
        suite::amend_ack,
        suite::mixed_batch,
        suite::unknown_on_timeout,
    ] {
        let started = Instant::now();
        let failure = failed(check(&subject(refusing)));
        assert!(
            says(&failure, "OrderEntryStub", "no request answered here"),
            "{failure}"
        );
        assert!(
            started.elapsed() < FAST,
            "refusing: {:?}",
            started.elapsed()
        );
    }
}
