//! FBC-0lng's done line (decision 0150): a serving script answers every data frame of every
//! connection through the responder made for that connection, several connections at once,
//! each in its own order; a refused answer fails itself alone and is reported as a script
//! failure; its pusher sends an open connection unsolicited frames and closes it, with or
//! without a code, and is refused on a closed one; and the record still tells a client's close
//! from a dropped socket. Step scripts are untouched: their own tests run unchanged.
//!
//! And FBC-4ptk's (decisions 0025 and 0150), each ordered by the client's reads and the stub's
//! records, never by a time: a pusher's barrier resolves only once the client has read every
//! frame asked before it, two on one connection each resolve in order, and one on a closed
//! connection resolves false; a responder's first answer held until another connection's close
//! or end is recorded is released by it, with each refusal of a hold failing that answer alone.

use std::io::ErrorKind;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use fbc_conformance::{
    ConnectionsView, Frame, HttpRoutes, PushError, Responder, ScriptError, Step, StubServer,
    WsScript,
};
use fbc_runtime::ws::{Message, WebSocket};
use fbc_runtime::{Connector, ProxyConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn connector() -> Connector {
    Connector::new(ProxyConfig::Direct)
}

/// Lets every task run until the runtime is idle (paused time jumps only then).
async fn settle() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}

async fn open(server: &StubServer) -> WebSocket {
    connector().websocket(&server.ws_url("/ws")).await.unwrap()
}

async fn send(ws: &mut WebSocket, text: &str) {
    ws.send(Message::text(text)).await.unwrap();
}

/// How long, on the wall clock, a test waits for what the stub owes it before failing. Paused
/// time is no bound: it jumps whenever the runtime is idle, even while the kernel still holds a
/// frame, and a stub that never answers leaves the runtime idle for ever.
const PATIENCE: std::time::Duration = Duration::from_secs(30);

/// `wait`, or a failure naming `what` once [`PATIENCE`] has passed on the wall clock, so a
/// regression (connections served one at a time, a connection no longer served) fails by name
/// instead of hanging the run. It yields meanwhile, so the runtime keeps polling its sockets.
async fn within<T>(what: &str, wait: impl Future<Output = T>) -> T {
    let deadline = std::time::Instant::now() + PATIENCE;
    let expired = async {
        while std::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        done = wait => done,
        () = expired => panic!("waited {PATIENCE:?} for {what}"),
    }
}

/// The next data frame the stub sent, skipping pings and pongs.
async fn recv(ws: &mut WebSocket) -> Message {
    within("the stub's next data frame", async {
        loop {
            let message = ws.next().await.unwrap().unwrap();
            if !message.is_ping() && !message.is_pong() {
                return message;
            }
        }
    })
    .await
}

async fn recv_text(ws: &mut WebSocket) -> String {
    recv(ws).await.into_text().unwrap().as_str().to_owned()
}

/// Waits until `done` holds of what the stub recorded. What a client sends that the stub
/// answers with nothing (a dropped socket, a refused frame) gives the test no frame to wait on,
/// and paused time jumps past what the kernel has not yet delivered, so this yields, and the
/// runtime polls its sockets, until the record says so, failing by `what` past [`PATIENCE`].
async fn until(what: &str, done: impl Fn() -> bool) {
    within(what, async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
}

/// Polls `f` once, without waiting: whether it has resolved at this point of the client's reads.
fn poll_once<F: Future>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Context::from_waker(Waker::noop()))
}

/// The next message the stub sent, a ping included.
async fn next(ws: &mut WebSocket) -> Message {
    within("the stub's next message", ws.next())
        .await
        .unwrap()
        .unwrap()
}

/// Reads the client's side to its end, so its close reply goes.
async fn drain(ws: &mut WebSocket) {
    within("the connection's end", async {
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await
}

/// Connection `conn`'s responder, a venue model that keeps its own state (0047): it numbers the
/// text frames it answers, `<conn>:<count>:<text>`, refuses `bad`, panics on `panic`, and
/// answers a binary frame with nothing.
fn numbered(conn: usize) -> Responder {
    let count = AtomicUsize::new(0);
    Responder::new(move |frame| match frame {
        Frame::Text(text) if text == "bad" => Err(format!("refused {text}")),
        Frame::Text(text) if text == "panic" => panic!("unparsable"),
        Frame::Text(text) => {
            let n = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(vec![Frame::text(format!("{conn}:{n}:{text}"))])
        }
        Frame::Binary(_) => Ok(Vec::new()),
    })
}

#[tokio::test(start_paused = true)]
async fn two_connections_are_served_at_once_each_answered_in_its_own_order() {
    let (script, _pusher) = WsScript::serving(numbered);
    assert!(script.is_serving());
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let mut trade = open(&server).await;
    let mut reads = open(&server).await;
    // The two are open at once and their frames interleave; each is answered by its own
    // responder, in the order it sent them.
    send(&mut trade, "place").await;
    send(&mut reads, "page").await;
    trade.send(Message::binary(vec![1])).await.unwrap();
    send(&mut trade, "cancel").await;
    send(&mut reads, "page").await;
    assert_eq!(server.live(), 2);
    assert_eq!(recv_text(&mut reads).await, "1:1:page");
    assert_eq!(recv_text(&mut trade).await, "0:1:place");
    assert_eq!(recv_text(&mut trade).await, "0:2:cancel");
    assert_eq!(recv_text(&mut reads).await, "1:2:page");
    // A connection opened later is served too, by a responder of its own.
    let mut later = open(&server).await;
    send(&mut later, "hello").await;
    assert_eq!(recv_text(&mut later).await, "2:1:hello");
    settle().await;
    // Every frame was answered, the binary one with nothing, and every one recorded.
    assert_eq!(server.played(), 6);
    let conns = server.connections();
    let texts = |all: &[&str]| all.iter().map(|t| Frame::text(*t)).collect::<Vec<_>>();
    let trade_sent = [
        Frame::text("place"),
        Frame::Binary(vec![1]),
        Frame::text("cancel"),
    ];
    assert_eq!(conns[0].received, trade_sent);
    assert_eq!(conns[1].received, texts(&["page", "page"]));
    assert_eq!(conns[2].received, texts(&["hello"]));
    assert!(conns.iter().all(|c| c.upgraded && c.open));
    assert!(server.failures().is_empty());
    // It serves until the stub is dropped, which closes every connection it held.
    drop(server);
    for ws in [&mut trade, &mut reads, &mut later] {
        let end = within("the dropped stub's close", ws.next()).await;
        assert!(matches!(end, None | Some(Err(_))));
    }
    settle().await;
}

#[tokio::test(start_paused = true)]
async fn a_refused_answer_fails_only_itself_and_is_reported_as_a_script_failure() {
    let (script, _pusher) = WsScript::serving(|conn| match conn {
        2 => panic!("no model for 2"),
        _ => numbered(conn),
    });
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let mut first = open(&server).await;
    let mut second = open(&server).await;
    // A refusal fails its answer alone: the next frame on that connection is answered.
    send(&mut first, "bad").await;
    send(&mut first, "good").await;
    assert_eq!(recv_text(&mut first).await, "0:1:good");
    let refused = ScriptError::Answer {
        conn: 0,
        frame: 0,
        reason: "refused bad".to_owned(),
    };
    // It is the script's failure: a serving script finishes only by failing.
    let first = within("the script's first failure", server.finished()).await;
    assert_eq!(first.unwrap_err(), refused);
    let shown = "connection 0, frame 0: the responder failed: refused bad";
    assert_eq!(refused.to_string(), shown);
    // The other connection is still served, and a panic in its responder is caught and
    // reported as a refusal of that answer alone.
    send(&mut second, "place").await;
    send(&mut second, "panic").await;
    send(&mut second, "again").await;
    assert_eq!(recv_text(&mut second).await, "1:1:place");
    assert_eq!(recv_text(&mut second).await, "1:2:again");
    // A connection whose responder could not be made has each answer refused, and the next
    // connection is served.
    let mut third = open(&server).await;
    send(&mut third, "x").await;
    send(&mut third, "y").await;
    let mut fourth = open(&server).await;
    send(&mut fourth, "z").await;
    assert_eq!(recv_text(&mut fourth).await, "3:1:z");
    // Every frame counts as answered, a refused one too.
    until("eight frames answered", || server.played() == 8).await;
    let unmade = |frame| ScriptError::Answer {
        conn: 2,
        frame,
        reason: "making its responder panicked: no model for 2".to_owned(),
    };
    let panicked = ScriptError::Answer {
        conn: 1,
        frame: 1,
        reason: "panicked: unparsable".to_owned(),
    };
    let failures = [refused.clone(), panicked, unmade(0), unmade(1)];
    assert_eq!(server.failures(), failures);
    // The first failure stays the script's result.
    assert_eq!(server.finished().await.unwrap_err(), refused);
    assert_eq!(server.live(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_push_reaches_an_open_connection_and_is_refused_on_a_closed_one() {
    let (script, pusher) = WsScript::serving(numbered);
    let early = Frame::text("early");
    // A script that runs on no stub has no connection to push to.
    let unbound = pusher.push(0, early.clone()).await;
    assert_eq!(unbound, Err(PushError::NoConnection { conn: 0 }));
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    // Connection 0 is no WebSocket: its upgrade fails, and it is never served or pushed to.
    let addr = server.ws_url("").replace("ws://", "");
    let mut raw = TcpStream::connect(addr).await.unwrap();
    raw.write_all(b"GET / HTTP/1.1\r\nHost: stub\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    let _ = raw.read_to_end(&mut answer).await;
    settle().await;
    assert!(!server.connections()[0].upgraded);
    let failed = pusher.push(0, early.clone()).await;
    assert_eq!(failed, Err(PushError::NoConnection { conn: 0 }));
    // Connection 1 is served by the responder made for it, and takes pushes from a clone of
    // the pusher, among its answers in the order asked.
    let mut ws = open(&server).await;
    send(&mut ws, "place").await;
    assert_eq!(recv_text(&mut ws).await, "1:1:place");
    let held = pusher.clone();
    held.push(1, Frame::text("fill")).await.unwrap();
    held.push(1, Frame::Binary(vec![7])).await.unwrap();
    send(&mut ws, "cancel").await;
    assert_eq!(recv_text(&mut ws).await, "fill");
    assert_eq!(recv(&mut ws).await, Message::binary(vec![7]));
    assert_eq!(recv_text(&mut ws).await, "1:2:cancel");
    // A connection never accepted refuses it.
    let none = pusher.push(7, early.clone()).await;
    assert_eq!(none, Err(PushError::NoConnection { conn: 7 }));
    // Once the client has gone, the push is refused and nothing is sent.
    drop(ws);
    until("connection 1's drop", || !server.connections()[1].open).await;
    let closed = pusher.push(1, early).await;
    assert_eq!(closed, Err(PushError::Closed { conn: 1 }));
    assert!(server.failures().is_empty());
    let shown = [
        PushError::NoConnection { conn: 7 }.to_string(),
        PushError::Closed { conn: 1 }.to_string(),
    ];
    let expected = [
        "the stub upgraded no connection 7",
        "connection 1 is closed",
    ];
    assert_eq!(shown, expected);
    assert_eq!(format!("{pusher:?}"), "Pusher(..)");
}

#[tokio::test(start_paused = true)]
async fn close_and_close_with_close_an_open_connection_and_are_refused_on_a_closed_one() {
    let (script, pusher) = WsScript::serving(numbered);
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let mut plain = open(&server).await;
    let mut coded = open(&server).await;
    send(&mut plain, "place").await;
    assert_eq!(recv_text(&mut plain).await, "0:1:place");
    send(&mut coded, "place").await;
    assert_eq!(recv_text(&mut coded).await, "1:1:place");

    // A close with no code; a frame the client sent across it is still recorded, and so is
    // the client's close reply.
    pusher.close(0).await.unwrap();
    send(&mut plain, "late").await;
    assert_eq!(recv(&mut plain).await, Message::Close(None));
    drain(&mut plain).await;

    // A close carrying a code and a reason.
    pusher
        .close_with(1, 4031, "no active subscriptions")
        .await
        .unwrap();
    let Message::Close(Some(close)) = recv(&mut coded).await else {
        panic!("a close frame with a body");
    };
    assert_eq!(u16::from(close.code), 4031);
    assert_eq!(close.reason.as_str(), "no active subscriptions");
    drain(&mut coded).await;
    settle().await;

    let conns = server.connections();
    let sent = [Frame::text("place"), Frame::text("late")];
    assert_eq!(conns[0].received, sent);
    for conn in &conns {
        assert!(
            conn.upgraded && !conn.open && conn.client_closed,
            "{conn:?}"
        );
    }
    // Each is refused on the closed connection, as a push is.
    for conn in 0..2 {
        let refused = Err(PushError::Closed { conn });
        assert_eq!(pusher.close(conn).await, refused);
        assert_eq!(pusher.close_with(conn, 1000, "bye").await, refused);
        assert_eq!(pusher.push(conn, Frame::text("x")).await, refused);
    }
    // The answer to "late" found the connection closing: it was not sent, failed nothing, and
    // counts as answered.
    assert!(server.failures().is_empty());
    assert_eq!(server.played(), 3);
}

#[tokio::test(start_paused = true)]
async fn client_closed_still_tells_a_close_from_a_drop() {
    let (script, pusher) = WsScript::serving(numbered);
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let view = server.connections_view();
    let mut graceful = open(&server).await;
    send(&mut graceful, "one").await;
    assert_eq!(recv_text(&mut graceful).await, "0:1:one");
    graceful.close(None).await.unwrap();
    drain(&mut graceful).await;
    let mut dropped = open(&server).await;
    send(&mut dropped, "two").await;
    assert_eq!(recv_text(&mut dropped).await, "1:1:two");
    drop(dropped);
    until("connection 1's drop", || !server.connections()[1].open).await;
    let seen = view.connections();
    assert_eq!(seen, server.connections());
    assert!(!seen[0].open && seen[0].client_closed, "{:?}", seen[0]);
    assert!(!seen[1].open && !seen[1].client_closed, "{:?}", seen[1]);
    for conn in 0..2 {
        let refused = Err(PushError::Closed { conn });
        assert_eq!(pusher.push(conn, Frame::text("x")).await, refused);
    }
}

#[tokio::test(start_paused = true)]
async fn a_serving_script_takes_no_steps_and_runs_on_one_stub() {
    let (script, _pusher) = WsScript::serving(numbered);
    // Clones share the pusher, and compare equal; another serving script does not.
    let clone = script.clone();
    let clone_after = script.clone();
    assert_eq!(clone, script);
    let (other, _) = WsScript::serving(numbered);
    assert_ne!(other, script);
    assert_ne!(WsScript::new(Vec::new()), script);
    assert_eq!(
        format!("{script:?}"),
        "WsScript { steps: [], serving: Serving(..) }"
    );
    // A step script shows as it always did.
    let steps = WsScript::new(vec![Step::Accept]);
    assert!(!steps.is_serving());
    assert_eq!(format!("{steps:?}"), "WsScript { steps: [Accept] }");

    // Given steps, it is refused.
    let mut stepped = other;
    stepped.steps.push(Step::Accept);
    let refused = StubServer::start(stepped, HttpRoutes::new()).await;
    let err = refused.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert_eq!(err.to_string(), "a serving script plays no steps");

    // It is started on one stub only: a clone started on a second is refused, and so is a clone
    // started once that stub is dropped.
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let again = StubServer::start(clone, HttpRoutes::new()).await;
    let err = again.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let started = "this serving script was already started on a stub";
    assert_eq!(err.to_string(), started);
    assert!(server.failures().is_empty());
    drop(server);
    let after = StubServer::start(clone_after, HttpRoutes::new()).await;
    assert_eq!(after.err().unwrap().to_string(), started);
}

#[tokio::test(start_paused = true)]
async fn a_step_script_lists_its_one_failure_and_a_finished_one_none() {
    let finished = StubServer::start(WsScript::default(), HttpRoutes::new())
        .await
        .unwrap();
    finished.finished().await.unwrap();
    assert!(finished.failures().is_empty());
    let script = WsScript::new(vec![Step::Read { conn: 0 }]);
    let failed = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let err = failed.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::NoConnection { step: 0, conn: 0 });
    assert_eq!(failed.failures(), [err]);
}

#[tokio::test(start_paused = true)]
async fn a_barrier_resolves_only_once_the_client_has_read_every_frame_asked_before_it() {
    let (script, pusher) = WsScript::serving(numbered);
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let mut ws = open(&server).await;
    // An answer and a push are asked before the barrier.
    send(&mut ws, "place").await;
    until("the answer to place", || server.played() == 1).await;
    pusher.push(0, Frame::text("fill")).await.unwrap();
    let mut barrier = pin!(pusher.barrier(0));
    assert!(poll_once(barrier.as_mut()).is_pending());
    // A push asked after it is written only once its ping was; the client has read nothing, so
    // the barrier still waits.
    pusher.push(0, Frame::text("after")).await.unwrap();
    assert!(poll_once(barrier.as_mut()).is_pending());
    assert_eq!(recv_text(&mut ws).await, "0:1:place");
    assert_eq!(recv_text(&mut ws).await, "fill");
    // The client reads the ping; its next read writes the pong.
    assert!(next(&mut ws).await.is_ping());
    assert_eq!(recv_text(&mut ws).await, "after");
    assert!(within("the barrier's pong", barrier).await);
    assert!(server.failures().is_empty());
}

#[tokio::test(start_paused = true)]
async fn two_barriers_on_one_connection_each_resolve_in_order() {
    let (script, pusher) = WsScript::serving(numbered);
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let mut ws = open(&server).await;
    until("connection 0's upgrade", || server.live() == 1).await;
    let mut first = pin!(pusher.barrier(0));
    assert!(poll_once(first.as_mut()).is_pending());
    pusher.push(0, Frame::text("between")).await.unwrap();
    let mut second = pin!(pusher.barrier(0));
    assert!(poll_once(second.as_mut()).is_pending());
    // Written once both pings were: both are pending together.
    pusher.push(0, Frame::text("last")).await.unwrap();
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());
    // The client reads the first ping, and its next read writes the pong: the first resolves,
    // while the second, whose ping the client has not read, still waits.
    assert!(next(&mut ws).await.is_ping());
    assert_eq!(recv_text(&mut ws).await, "between");
    assert!(within("the first barrier's pong", first).await);
    assert!(poll_once(second.as_mut()).is_pending());
    assert!(next(&mut ws).await.is_ping());
    assert_eq!(recv_text(&mut ws).await, "last");
    assert!(within("the second barrier's pong", second).await);
}

#[tokio::test(start_paused = true)]
async fn a_barrier_whose_ping_cannot_be_written_resolves_false_after_every_barrier_before_it() {
    let (script, pusher) = WsScript::serving(numbered);
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let mut ws = open(&server).await;
    until("connection 0's upgrade", || server.live() == 1).await;
    let mut first = pin!(pusher.barrier(0));
    assert!(poll_once(first.as_mut()).is_pending());
    // The stub's close: no later write on the connection goes, though it still reads pongs.
    pusher.close(0).await.unwrap();
    let mut second = pin!(pusher.barrier(0));
    assert!(poll_once(second.as_mut()).is_pending());
    // A push asked after the second barrier is refused, so its ping, which did not go, was
    // taken: it still waits for the first.
    let refused = pusher.push(0, Frame::text("after")).await;
    assert_eq!(refused, Err(PushError::Closed { conn: 0 }));
    assert!(poll_once(second.as_mut()).is_pending());
    // The client reads the first ping, and its next read, of the stub's close, writes the pong:
    // the first resolves true, and the second false behind it.
    assert!(next(&mut ws).await.is_ping());
    assert!(next(&mut ws).await.is_close());
    assert!(within("the first barrier's pong", first).await);
    assert_eq!(poll_once(second.as_mut()), Poll::Ready(false));

    // With no barrier before it, one whose ping cannot be written resolves false at once, while
    // the connection, whose client has read nothing, is still open.
    let _quiet = open(&server).await;
    let upgraded = || server.connections().get(1).is_some_and(|c| c.upgraded);
    until("connection 1's upgrade", upgraded).await;
    pusher.close(1).await.unwrap();
    assert!(!within("the unwritten barrier", pusher.barrier(1)).await);
    assert!(server.connections()[1].open);
    assert!(server.failures().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_barrier_on_a_closed_connection_resolves_false() {
    let (script, pusher) = WsScript::serving(numbered);
    // A script that runs on no stub has no connection.
    assert!(!pusher.barrier(0).await);
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    // A connection the client dropped.
    let dropped = open(&server).await;
    until("connection 0's upgrade", || server.live() == 1).await;
    drop(dropped);
    until("connection 0's drop", || !server.connections()[0].open).await;
    assert!(!pusher.barrier(0).await);
    // A connection never accepted.
    assert!(!pusher.barrier(7).await);
    // A connection that ends after the ping went and before the client's pong.
    let ending = open(&server).await;
    until("connection 1's upgrade", || server.live() == 1).await;
    let mut barrier = pin!(pusher.barrier(1));
    assert!(poll_once(barrier.as_mut()).is_pending());
    pusher.push(1, Frame::text("after")).await.unwrap();
    drop(ending);
    assert!(!within("the barrier's end", barrier).await);
    assert!(server.failures().is_empty());
}

/// Connection `conn`'s responder in the hold tests: it answers each text frame with
/// `<conn>:<text>:ended=[..]`, listing the connections whose record showed the client's close
/// frame or their end when the answer was computed.
fn seeing(conn: usize, view: Arc<OnceLock<ConnectionsView>>) -> Responder {
    Responder::new(move |frame| {
        let Frame::Text(text) = frame else {
            return Ok(Vec::new());
        };
        let conns = view.get().expect("the stub started").connections();
        let ended = conns.iter().enumerate();
        let ended = ended.filter(|(_, c)| c.client_closed || !c.open);
        let ended: Vec<String> = ended.map(|(at, _)| at.to_string()).collect();
        let answer = format!("{conn}:{text}:ended=[{}]", ended.join(","));
        Ok(vec![Frame::text(answer)])
    })
}

#[tokio::test(start_paused = true)]
async fn a_first_answer_held_until_another_connections_close_or_end_is_released_by_it() {
    let view = Arc::new(OnceLock::new());
    let (asked_tx, mut asked) = tokio::sync::mpsc::unbounded_channel();
    let made = view.clone();
    // Connection 2 reads once connection 0 has closed, and connection 3 once 1 has ended. Each
    // hold tells the test it was asked, as the first frame arrives.
    let (script, _pusher) = WsScript::serving(move |conn| {
        let with = seeing(conn, made.clone());
        let asked = asked_tx.clone();
        match conn {
            2 | 3 => with.hold_first(move |frame| {
                let _ = asked.send((conn, frame.clone()));
                Some(conn - 2)
            }),
            _ => with,
        }
    });
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    view.set(server.connections_view()).ok().unwrap();
    let mut closing = open(&server).await;
    send(&mut closing, "place").await;
    assert_eq!(recv_text(&mut closing).await, "0:place:ended=[]");
    let mut dropping = open(&server).await;
    send(&mut dropping, "place").await;
    assert_eq!(recv_text(&mut dropping).await, "1:place:ended=[]");

    // Connection 2's first frame arrives while connection 0 is open, and a second behind it.
    let mut after_close = open(&server).await;
    send(&mut after_close, "auth").await;
    send(&mut after_close, "page").await;
    let first = within("connection 2's hold", asked.recv()).await;
    assert_eq!(first, Some((2, Frame::text("auth"))));
    // Every other connection is served meanwhile.
    send(&mut dropping, "more").await;
    assert_eq!(recv_text(&mut dropping).await, "1:more:ended=[]");
    // Connection 0's close releases it: both answers are computed after the record showed it,
    // in order. The stub records the client's close frame and the connection's end in one turn
    // here, so this shows the release by both together; `src/serving.rs`'s unit test shows
    // each releasing it alone.
    closing.close(None).await.unwrap();
    assert_eq!(recv_text(&mut after_close).await, "2:auth:ended=[0]");
    assert_eq!(recv_text(&mut after_close).await, "2:page:ended=[0]");
    drain(&mut closing).await;

    // Connection 3's first answer waits for connection 1's end, a drop with no close frame.
    let mut after_drop = open(&server).await;
    send(&mut after_drop, "auth").await;
    let first = within("connection 3's hold", asked.recv()).await;
    assert_eq!(first, Some((3, Frame::text("auth"))));
    drop(dropping);
    assert_eq!(recv_text(&mut after_drop).await, "3:auth:ended=[0,1]");
    // Only the first answer is held: a later frame is answered at once.
    send(&mut after_close, "again").await;
    assert_eq!(recv_text(&mut after_close).await, "2:again:ended=[0,1]");
    assert!(server.failures().is_empty());
    assert_eq!(server.played(), 7);
}

#[tokio::test(start_paused = true)]
async fn a_refused_hold_fails_its_first_answer_alone_and_none_answers_at_once() {
    let (script, _pusher) = WsScript::serving(|conn| match conn {
        0 => numbered(0).hold_first(|_| Some(9)),
        1 => numbered(1).hold_first(|_| panic!("no hold")),
        _ => numbered(conn).hold_first(|_| None),
    });
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let mut unaccepted = open(&server).await;
    let mut panicking = open(&server).await;
    let mut none = open(&server).await;
    // A hold naming no connection answers at once, while connections 0 and 1 are open.
    send(&mut none, "auth").await;
    assert_eq!(recv_text(&mut none).await, "2:1:auth");
    // A hold naming a connection the stub has not accepted, or panicking, fails the first
    // answer alone: the next frame is answered.
    for ws in [&mut unaccepted, &mut panicking] {
        send(ws, "auth").await;
        send(ws, "page").await;
    }
    assert_eq!(recv_text(&mut unaccepted).await, "0:1:page");
    assert_eq!(recv_text(&mut panicking).await, "1:1:page");
    until("five frames answered", || server.played() == 5).await;
    let refused = |conn, reason: &str| ScriptError::Answer {
        conn,
        frame: 0,
        reason: reason.to_owned(),
    };
    let failures = server.failures();
    let waits = "its first answer waits for connection 9, not accepted";
    assert!(failures.contains(&refused(0, waits)), "{failures:?}");
    assert!(failures.contains(&refused(1, "its hold panicked: no hold")));
    assert_eq!(failures.len(), 2);

    // A step script refuses a responder with a hold when it starts.
    let held = numbered(0).hold_first(|_| Some(0));
    let steps = vec![
        Step::Accept,
        Step::Respond {
            conn: 0,
            with: held,
        },
    ];
    let err = StubServer::start(WsScript::new(steps), HttpRoutes::new()).await;
    let err = err.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let why = "a responder that holds its first answer plays only in a serving script";
    assert_eq!(err.to_string(), why);
}

#[tokio::test(start_paused = true)]
async fn a_hold_the_stubs_drop_ends_answers_nothing() {
    let (computed_tx, mut computed) = tokio::sync::mpsc::unbounded_channel();
    // Connection 1's responder tells each answer it computes; its drop, with the stub's last
    // task, ends the channel.
    let (script, _pusher) = WsScript::serving(move |conn| {
        let computed = computed_tx.clone();
        let with = Responder::new(move |_| {
            let _ = computed.send(conn);
            Ok(Vec::new())
        });
        match conn {
            1 => with.hold_first(|_| Some(0)),
            _ => with,
        }
    });
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();
    let _open = open(&server).await;
    let mut held = open(&server).await;
    send(&mut held, "auth").await;
    until("connection 1's first frame", || {
        server.connections()[1].received.len() == 1
    })
    .await;
    drop(server);
    // The held answer is never computed: the channel ends with nothing in it.
    assert_eq!(within("the stub's last task", computed.recv()).await, None);
}
