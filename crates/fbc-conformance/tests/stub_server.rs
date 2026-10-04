//! The stub server's own behaviour, driven through fbc-runtime's connector: each script step,
//! the record of connections and frames, the fixed HTTP responses, and every way a script fails.

use std::time::Duration;

use fbc_conformance::{Frame, HttpReply, HttpRoutes, ScriptError, Step, StubServer, WsScript};
use fbc_runtime::http::{Bytes, Method, Request, StatusCode};
use fbc_runtime::ws::Message;
use fbc_runtime::{Connector, ProxyConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn connector() -> Connector {
    Connector::new(ProxyConfig::Direct)
}

async fn stub(steps: Vec<Step>) -> StubServer {
    StubServer::start(WsScript::new(steps), HttpRoutes::new())
        .await
        .unwrap()
}

/// Lets every task run until the runtime is idle (paused time jumps only then).
async fn settle() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test(start_paused = true)]
async fn a_script_reads_pushes_and_closes_and_records_every_data_frame() {
    let server = stub(vec![
        Step::Accept,
        Step::Read { conn: 0 },
        Step::Push {
            conn: 0,
            frame: Frame::text("ack"),
        },
        Step::Push {
            conn: 0,
            frame: Frame::Binary(vec![1, 2]),
        },
        Step::Close { conn: 0 },
    ])
    .await;
    let mut ws = connector().websocket(&server.ws_url("/any")).await.unwrap();
    ws.send(Message::Ping(Vec::new().into())).await.unwrap();
    ws.send(Message::text("sub")).await.unwrap();
    ws.send(Message::binary(vec![9])).await.unwrap();
    let mut got = Vec::new();
    while let Some(Ok(message)) = ws.next().await {
        if !message.is_pong() {
            got.push(message);
        }
    }
    assert_eq!(
        got[..2],
        [Message::text("ack"), Message::binary(vec![1, 2])]
    );
    assert!(got[2].is_close());
    server.finished().await.unwrap();
    settle().await;
    let conns = server.connections();
    assert_eq!(conns.len(), 1);
    // The ping is protocol, not data; the binary frame was recorded though no step read it.
    let sent = [Frame::text("sub"), Frame::Binary(vec![9])];
    assert_eq!(conns[0].received, sent);
    assert!(conns[0].upgraded && !conns[0].open);
    assert_eq!(server.live(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_silent_connection_stays_open_reading_nothing_until_the_stub_is_dropped() {
    let server = stub(vec![
        Step::Accept,
        Step::Read { conn: 0 },
        Step::Silent { conn: 0 },
    ])
    .await;
    let mut ws = connector().websocket(&server.ws_url("/md")).await.unwrap();
    ws.send(Message::text("one")).await.unwrap();
    // Once the script has finished, the connection has gone silent (Codex r4177514250).
    server.finished().await.unwrap();
    ws.send(Message::text("two")).await.unwrap();
    settle().await;
    assert_eq!(server.connections()[0].received, [Frame::text("one")]);
    assert_eq!(server.live(), 1);
    // Dropping the stub closes the connection it held.
    drop(server);
    assert!(matches!(ws.next().await, None | Some(Err(_))));
}

#[tokio::test(start_paused = true)]
async fn a_step_naming_a_connection_not_yet_accepted_fails_the_script() {
    let server = stub(vec![Step::Read { conn: 0 }]).await;
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::NoConnection { step: 0, conn: 0 });
    assert_eq!(
        err.to_string(),
        "step 0 names connection 0, not yet accepted"
    );
}

#[tokio::test(start_paused = true)]
async fn a_connection_that_is_not_a_websocket_fails_its_accept_and_is_recorded() {
    let server = stub(vec![Step::Accept]).await;
    let addr = server.ws_url("").trim_start_matches("ws://").to_owned();
    let mut raw = TcpStream::connect(addr).await.unwrap();
    raw.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::Accept { step: 0, conn: 0 });
    assert_eq!(err.to_string(), "step 0: connection 0 was not upgraded");
    let conns = server.connections();
    assert!(!conns[0].upgraded && !conns[0].open);
    assert_eq!(server.live(), 0);
}

#[tokio::test(start_paused = true)]
async fn reading_from_or_pushing_to_a_closed_connection_fails_the_script() {
    let server = stub(vec![Step::Accept, Step::Read { conn: 0 }]).await;
    let ws = connector().websocket(&server.ws_url("/md")).await.unwrap();
    drop(ws);
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::Closed { step: 1, conn: 0 });
    assert_eq!(err.to_string(), "step 1: connection 0 is closed");

    let push = Step::Push {
        conn: 0,
        frame: Frame::text("late"),
    };
    let server = stub(vec![Step::Accept, Step::Close { conn: 0 }, push]).await;
    let _ws = connector().websocket(&server.ws_url("/md")).await.unwrap();
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::Closed { step: 2, conn: 0 });
}

#[tokio::test(start_paused = true)]
async fn closing_a_connection_the_client_already_closed_fails_the_script() {
    // Steps name their connections, so a script can act on an older one while a newer is open.
    let server = stub(vec![
        Step::Accept,
        Step::Accept,
        Step::Read { conn: 1 },
        Step::Close { conn: 0 },
    ])
    .await;
    let first = connector().websocket(&server.ws_url("/md")).await.unwrap();
    let mut second = connector().websocket(&server.ws_url("/md")).await.unwrap();
    drop(first);
    settle().await;
    second.send(Message::text("go")).await.unwrap();
    // The client closed first: the stub did not force this close (Codex r4177464414).
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::Closed { step: 3, conn: 0 });
    assert_eq!(server.live(), 1);
}

#[tokio::test(start_paused = true)]
async fn the_http_endpoint_answers_fixed_responses_by_path_and_refuses_what_it_cannot_read() {
    let routes = HttpRoutes::from([(
        "/markets".to_owned(),
        HttpReply {
            status: 200,
            body: b"[1]".to_vec(),
        },
    )]);
    let server = StubServer::start(WsScript::default(), routes)
        .await
        .unwrap();
    let call = |method: Method, path: &str, body: &'static [u8]| {
        let request = Request::builder()
            .method(method)
            .uri(server.http_url(path))
            .body(Bytes::from_static(body))
            .unwrap();
        async move { connector().http(request, 1024).await.unwrap() }
    };
    let found = call(Method::GET, "/markets?x=1", b"").await;
    assert_eq!(found.status(), StatusCode::OK);
    assert_eq!(found.body().as_ref(), b"[1]");
    let missing = call(Method::GET, "/nope", b"").await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert!(missing.body().is_empty());
    let posted = call(Method::POST, "/markets", b"{\"a\":1}").await;
    assert_eq!(posted.status(), StatusCode::OK);
    // A body that arrives after its head is read to its stated length before the answer, and
    // its request keeps its place in arrival order though a later one finished first (Codex
    // r4177464418).
    let addr = server.http_url("").trim_start_matches("http://").to_owned();
    let mut raw = TcpStream::connect(&addr).await.unwrap();
    let head = b"PUT /markets HTTP/1.1\r\nContent-Length: 3\r\n\r\n";
    raw.write_all(head).await.unwrap();
    settle().await;
    let later = call(Method::GET, "/later", b"").await;
    assert_eq!(later.status(), StatusCode::NOT_FOUND);
    raw.write_all(b"abc").await.unwrap();
    let mut answer = String::new();
    raw.read_to_string(&mut answer).await.unwrap();
    assert!(answer.starts_with("HTTP/1.1 200 ") && answer.ends_with("[1]"));
    assert_eq!(
        server.http_requests(),
        [
            "GET /markets?x=1",
            "GET /nope",
            "POST /markets",
            "PUT /markets",
            "GET /later"
        ]
    );

    // A request line without a target, a bad length, a head that never ends within the limit,
    // a head that ends just past it (Codex r4177464412), and a body longer than the limit are
    // each answered 400 and not recorded.
    let too_long = format!("POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n", 1 << 20);
    let endless = "x".repeat(70 * 1024);
    let big_head = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "y".repeat(65_600));
    for request in [
        "JUNK\r\n\r\n",
        "GET / HTTP/1.1\r\nContent-Length: many\r\n\r\n",
        too_long.as_str(),
        endless.as_str(),
        big_head.as_str(),
    ] {
        let mut raw = TcpStream::connect(&addr).await.unwrap();
        raw.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        raw.read_to_string(&mut answer).await.unwrap();
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }
    assert_eq!(server.http_requests().len(), 5);
}
