//! FBC-klr's done line, for one endpoint: a codec's HTTP request comes back to its `on_http` as
//! a response with its status and headers, or as `TimedOut`, `NotSent` or `Lost`; a response
//! that arrives after its stream reconnected reaches no codec and is counted; and a poll
//! endpoint opens no socket, delivers what it polls and refuses a frame or a reconnect.

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use common::toy::{self, ToyVenue};
use common::{ScriptedHttp, ScriptedWs, closed_port, once_then_hanging};
use fbc_core::{ConnKey, EndpointPlan, Envelope, MdEvent, MdTransport, VenueConfig, WireUrl};
use fbc_runtime::{
    Connector, IngestClock, Input, MdSession, MdSessionConfig, ProxyConfig, ReconnectPacing,
};
use tokio::time::Instant;

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push(env)
}

const CONN: u16 = 3;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A session of `venue` on `transport`, wanting trades on instrument 1.
fn session(venue: &'static ToyVenue, transport: MdTransport) -> MdSessionConfig {
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport,
            subs: vec![toy::sub(1)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), ms(5_000)).unwrap(),
        clock: IngestClock::new(),
        http_max_body: 64 * 1024,
        conn: CONN,
    }
}

fn socket(url: String) -> MdTransport {
    MdTransport::Socket {
        url: WireUrl::plain(url),
    }
}

async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

#[tokio::test]
async fn an_answered_request_reaches_on_http_with_its_status_and_headers() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::leak();
    let seen = Seen::default();
    let (mut session, control) =
        MdSession::new(session(venue, socket(ws.url())), keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send(&format!("get|tag=1|ms=5000|url={}", http.url("/snap")));
        let exchange = http.request().await;
        assert_eq!(exchange.line, "GET /snap");
        let head = "HTTP/1.1 203 Non-Authoritative Information\r\nX-Toy: yes";
        exchange
            .answer(head, "trade|sym=A|px=7|qty=1|seq=4\nsay")
            .await;
        // The effects on_http asked for are executed on the socket.
        assert_eq!(peer.recv().await, "said");
        until(|| watch.borrow().len() == 1).await;
        // A response the codec cannot decode is counted.
        peer.send(&format!("get|tag=7|ms=5000|url={}", http.url("/bad")));
        let head = "HTTP/1.1 500 Internal Server Error";
        http.request().await.answer(head, "junk").await;
        until(|| venue.http_log().len() == 2).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(venue.http_log(), ["0/1:203:yes", "0/7:500:-"]);
    assert_eq!(session.counters().decode_errors, 1);
    let seen = seen.borrow();
    assert_eq!((seen[0].stamp.conn, seen[0].venue_seq), (key(0), Some(4)));
    assert_eq!(session.stale(Input::Http), 0);
}

#[tokio::test]
async fn an_unanswered_request_times_out_and_a_refused_or_lost_one_says_so() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, socket(ws.url())), |_| {}).unwrap();
    let closed = format!("http://127.0.0.1:{}/x", closed_port().await);
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // Never answered: TimedOut once its 150 ms have passed.
        let start = Instant::now();
        peer.send(&format!("get|tag=2|ms=150|url={}", http.url("/slow")));
        let held = http.request().await;
        until(|| venue.http_log().len() == 1).await;
        assert!(start.elapsed() >= ms(150));
        drop(held);
        // Refused at connect: no byte written.
        peer.send(&format!("get|tag=3|ms=5000|url={closed}"));
        until(|| venue.http_log().len() == 2).await;
        // A URL the runtime cannot call: no byte written either.
        peer.send("get|tag=4|ms=5000|url=ftp://127.0.0.1/x");
        until(|| venue.http_log().len() == 3).await;
        // Written, then the connection closed with no answer.
        peer.send(&format!("get|tag=5|ms=5000|url={}", http.url("/lost")));
        drop(http.request().await);
        until(|| venue.http_log().len() == 4).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let log = venue.http_log();
    assert_eq!(
        log,
        ["0/2:TimedOut", "0/3:NotSent", "0/4:NotSent", "0/5:Lost"]
    );
}

#[tokio::test]
async fn a_response_that_arrives_after_its_stream_reconnected_is_dropped_and_counted() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::leak();
    let seen = Seen::default();
    let (mut session, control) =
        MdSession::new(session(venue, socket(ws.url())), keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut first = ws.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1");
        assert_eq!(first.recv().await, "sub|add=A");
        first.send(&format!("get|tag=6|ms=5000|url={}", http.url("/snapshot")));
        let snapshot = http.request().await;
        first.send("bye");
        assert_eq!(first.next().await, None);
        let mut second = ws.accept().await;
        assert_eq!(second.recv().await, "hello|codec=1|plan=1");
        assert_eq!(second.recv().await, "sub|add=A");
        // The old epoch's snapshot arrives now; it must not anchor the new epoch.
        let head = "HTTP/1.1 200 OK";
        snapshot.answer(head, "trade|sym=A|px=1|qty=1|seq=1").await;
        tokio::time::sleep(ms(50)).await;
        second.send("trade|sym=A|px=2|qty=1|seq=2");
        until(|| watch.borrow().len() == 1).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(venue.http_log().is_empty(), "{:?}", venue.http_log());
    let seen = seen.borrow();
    assert_eq!(seen.len(), 1);
    assert_eq!((seen[0].stamp.conn, seen[0].venue_seq), (key(1), Some(2)));
    assert_eq!(session.stale(Input::Http), 1);
}

/// The first connection asks for a GET that times out after 200 ms, then for a reconnect;
/// later attempts hang. With `floor`, the result comes back while the session waits to
/// reconnect (a long floor) or while its next attempt hangs (a short one).
async fn an_ended_epochs_request_comes_back_into_nothing(floor: Duration) {
    let mut http = ScriptedHttp::start().await;
    let get = format!("get|tag=8|ms=200|url={}", http.url("/late"));
    let texts: &'static [&'static str] = Box::leak(Box::new([get.leak() as &str, "bye"]));
    let (addr, mut accepts) = once_then_hanging(texts).await;
    let venue = ToyVenue::leak();
    let mut config = session(venue, socket(format!("ws://{addr}/md")));
    config.pacing = ReconnectPacing::new(floor, floor, 100, ms(60_000), ms(5_000)).unwrap();
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let held = http.request().await;
        accepts.recv().await.unwrap();
        tokio::time::sleep(ms(400)).await;
        drop((held, control));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(venue.http_log().is_empty(), "{:?}", venue.http_log());
    assert_eq!(session.stale(Input::Http), 1);
}

#[tokio::test]
async fn a_request_of_an_ended_epoch_comes_back_into_nothing_while_disconnected() {
    an_ended_epochs_request_comes_back_into_nothing(ms(300)).await;
}

#[tokio::test]
async fn a_request_of_an_ended_epoch_comes_back_into_nothing_while_an_attempt_hangs() {
    an_ended_epochs_request_comes_back_into_nothing(ms(10)).await;
}

#[tokio::test]
async fn a_poll_endpoint_opens_no_socket_delivers_what_it_polls_and_refuses_frames() {
    let mut http = ScriptedHttp::start().await;
    let venue = ToyVenue::leak();
    let seen = Seen::default();
    let poll = MdTransport::Poll {
        base_url: WireUrl::plain(http.url("")),
    };
    let (mut session, control) = MdSession::new(session(venue, poll), keep(&seen)).unwrap();
    let watch = seen.clone();
    let mut lines = Vec::new();
    let script = async {
        let ok = "HTTP/1.1 200 OK";
        let first = http.request().await;
        lines.push(first.line.clone());
        // A frame and a reconnect named for the poll stream: refused, nothing written.
        first
            .answer(ok, "trade|sym=A|px=9|qty=1|seq=1\nsay\nbye")
            .await;
        let second = http.request().await;
        lines.push(second.line.clone());
        second.answer(ok, "trade|sym=A|px=10|qty=1|seq=2").await;
        until(|| watch.borrow().len() == 2).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    while let Some(late) = http.try_request() {
        lines.push(late.line.clone());
    }
    // Every connection the endpoint made was one of its polls, subscribed as planned.
    assert!(lines.iter().all(|l| l == "GET /poll?syms=A"), "{lines:?}");
    assert!(lines.len() >= 2 && http.connections() >= lines.len());
    let counters = session.counters();
    assert_eq!((counters.attempts, counters.refused_effects), (0, 2));
    assert_eq!(&venue.http_log()[..2], ["0/0:200:-", "0/0:200:-"]);
    let seen = seen.borrow();
    let seqs: Vec<_> = seen.iter().map(|e| (e.stamp.conn, e.venue_seq)).collect();
    assert_eq!(seqs, [(key(0), Some(1)), (key(0), Some(2))]);
    assert_eq!(session.current(), key(0));
}
