//! FBC-xg7's done line: the stub answers what it reads. A script step reads a client's frame and
//! pushes the frames a function of that frame returns (a reply echoing the request id, a batch's
//! outcome per item in the order it named them), and the HTTP endpoint answers by method and
//! path pattern with a reply computed from the request. The client is fbc-runtime's connector,
//! driven by the test.

use fbc_conformance::{
    Frame, HttpReply, HttpRequest, HttpRouter, PathPattern, PatternError, Responder, ScriptError,
    Step, StubServer, WsScript,
};
use fbc_runtime::http::{Bytes, Method, Request, Response, StatusCode};
use fbc_runtime::ws::Message;
use fbc_runtime::{Connector, ProxyConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn connector() -> Connector {
    Connector::new(ProxyConfig::Direct)
}

/// The value of `key` in a `kind|key=value|...` record.
fn field<'a>(record: &'a str, key: &str) -> Option<&'a str> {
    record
        .split('|')
        .skip(1)
        .filter_map(|kv| kv.split_once('='))
        .find_map(|(k, v)| (k == key).then_some(v))
}

fn text(frame: &Frame) -> Result<&str, String> {
    match frame {
        Frame::Text(text) => Ok(text),
        Frame::Binary(_) => Err("a binary frame".to_owned()),
    }
}

/// Answers a request record with an acknowledgement echoing its id: `ack|id=<id>`.
fn echo_id() -> Responder {
    Responder::new(|frame: &Frame| {
        let id = field(text(frame)?, "id").ok_or("no id")?;
        Ok(vec![Frame::text(format!("ack|id={id}"))])
    })
}

/// Answers a batch `batch|id=<id>|items=<a>,<b>,...` with one outcome frame per item, in the
/// order the batch named them; an item named `bad` is rejected.
fn batch_outcomes() -> Responder {
    Responder::new(|frame: &Frame| {
        let record = text(frame)?;
        let id = field(record, "id").ok_or("no id")?;
        let items = field(record, "items").ok_or("no items")?;
        let outcome = |(n, item): (usize, &str)| {
            let status = if item == "bad" { "rejected" } else { "ok" };
            Frame::text(format!("outcome|id={id}|n={n}|item={item}|status={status}"))
        };
        Ok(items.split(',').enumerate().map(outcome).collect())
    })
}

async fn recv_text(ws: &mut fbc_runtime::ws::WebSocket) -> String {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return text.as_str().to_owned(),
            Some(Ok(_)) => continue,
            other => panic!("the stub sent no text frame: {other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_script_answers_each_frame_it_reads_with_a_reply_carrying_its_request_id() {
    let respond = |conn| Step::Respond {
        conn,
        with: echo_id(),
    };
    let script = WsScript::new(vec![Step::Accept, respond(0), respond(0), respond(0)]);
    let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
    let mut ws = connector().websocket(&server.ws_url("/rpc")).await.unwrap();
    // The ids are not in order, so only a reply computed from each request can carry its own.
    for id in ["17", "3", "abc-9"] {
        ws.send(Message::text(format!("place|id={id}|px=100")))
            .await
            .unwrap();
        assert_eq!(recv_text(&mut ws).await, format!("ack|id={id}"));
    }
    server.finished().await.unwrap();
    // Every frame read is recorded, as a plain Read records it.
    let received = &server.connections()[0].received;
    assert_eq!(received.len(), 3);
    assert_eq!(received[2], Frame::text("place|id=abc-9|px=100"));
}

#[tokio::test(start_paused = true)]
async fn a_batch_is_answered_with_one_outcome_per_item_in_the_order_it_named_them() {
    let script = WsScript::new(vec![
        Step::Accept,
        Step::Respond {
            conn: 0,
            with: batch_outcomes(),
        },
    ]);
    let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
    let mut ws = connector().websocket(&server.ws_url("/rpc")).await.unwrap();
    ws.send(Message::text("batch|id=8|items=z9,bad,a1"))
        .await
        .unwrap();
    let mut outcomes = Vec::new();
    for _ in 0..3 {
        outcomes.push(recv_text(&mut ws).await);
    }
    assert_eq!(
        outcomes,
        [
            "outcome|id=8|n=0|item=z9|status=ok",
            "outcome|id=8|n=1|item=bad|status=rejected",
            "outcome|id=8|n=2|item=a1|status=ok",
        ]
    );
    server.finished().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_responder_may_push_nothing_and_steps_compare_by_the_responder_they_hold() {
    let silent = Responder::new(|_: &Frame| Ok(Vec::new()));
    let step = Step::Respond {
        conn: 0,
        with: silent.clone(),
    };
    // A clone holds the same function; another responder, even one written alike, does not.
    assert_eq!(
        step,
        Step::Respond {
            conn: 0,
            with: silent.clone()
        }
    );
    assert_ne!(
        step,
        Step::Respond {
            conn: 0,
            with: Responder::new(|_: &Frame| Ok(Vec::new())),
        }
    );
    assert_eq!(format!("{silent:?}"), "Responder(..)");
    let script = WsScript::new(vec![
        Step::Accept,
        step,
        Step::Push {
            conn: 0,
            frame: Frame::text("after"),
        },
    ]);
    let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
    let mut ws = connector().websocket(&server.ws_url("/rpc")).await.unwrap();
    ws.send(Message::binary(vec![1])).await.unwrap();
    // The responder pushed nothing, so the next frame is the script's own push.
    assert_eq!(recv_text(&mut ws).await, "after");
    server.finished().await.unwrap();
    assert_eq!(server.connections()[0].received, [Frame::Binary(vec![1])]);
}

#[tokio::test(start_paused = true)]
async fn a_responder_that_refuses_or_panics_fails_the_script_naming_the_step() {
    // A refusal: the frame read is not what the responder answers.
    let script = WsScript::new(vec![
        Step::Accept,
        Step::Respond {
            conn: 0,
            with: echo_id(),
        },
    ]);
    let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
    let mut ws = connector().websocket(&server.ws_url("/rpc")).await.unwrap();
    ws.send(Message::text("place|px=1")).await.unwrap();
    let err = server.finished().await.unwrap_err();
    let refused = ScriptError::Respond {
        step: 1,
        conn: 0,
        reason: "no id".to_owned(),
    };
    assert_eq!(err, refused);
    assert_eq!(
        err.to_string(),
        "step 1: the responder on connection 0 failed: no id"
    );
    drop(ws);

    // A panic is caught and reported, never taken for a script that finished; a panic payload
    // that is a String or neither kind of string is reported too.
    let panics = [
        Responder::new(|_: &Frame| panic!("unparsable")),
        Responder::new(|_: &Frame| std::panic::panic_any(String::from("owned"))),
        Responder::new(|_: &Frame| std::panic::panic_any(7_u8)),
    ];
    for (with, reason) in panics.into_iter().zip(["unparsable", "owned", "a panic"]) {
        let script = WsScript::new(vec![Step::Accept, Step::Respond { conn: 0, with }]);
        let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
        let mut ws = connector().websocket(&server.ws_url("/rpc")).await.unwrap();
        ws.send(Message::text("x")).await.unwrap();
        let err = server.finished().await.unwrap_err();
        let panicked = ScriptError::Respond {
            step: 1,
            conn: 0,
            reason: format!("panicked: {reason}"),
        };
        assert_eq!(err, panicked);
    }
}

#[tokio::test(start_paused = true)]
async fn a_respond_step_fails_on_a_connection_closed_or_never_accepted() {
    let respond = |conn| Step::Respond {
        conn,
        with: echo_id(),
    };
    let server = StubServer::start(WsScript::new(vec![respond(0)]), HttpRouter::new())
        .await
        .unwrap();
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::NoConnection { step: 0, conn: 0 });

    // The client leaves before sending anything: there is no frame to answer.
    let script = WsScript::new(vec![Step::Accept, respond(0)]);
    let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
    drop(connector().websocket(&server.ws_url("/rpc")).await.unwrap());
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::Closed { step: 1, conn: 0 });

    // The connection closes after the request arrived: the request is still read, and the
    // step fails on its push.
    let script = WsScript::new(vec![
        Step::Accept,
        Step::Read { conn: 0 },
        Step::Close { conn: 0 },
        respond(0),
    ]);
    let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
    let mut ws = connector().websocket(&server.ws_url("/rpc")).await.unwrap();
    ws.send(Message::text("first")).await.unwrap();
    ws.send(Message::text("place|id=1")).await.unwrap();
    while let Some(Ok(_)) = ws.next().await {}
    let err = server.finished().await.unwrap_err();
    assert_eq!(err, ScriptError::Closed { step: 3, conn: 0 });
    let received = &server.connections()[0].received;
    assert_eq!(received[1], Frame::text("place|id=1"));
}

/// Calls the stub with `method` on `path`, sending `body` and the headers given.
async fn call(
    server: &StubServer,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: &'static [u8],
) -> Response<Bytes> {
    let mut request = Request::builder().method(method).uri(server.http_url(path));
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let request = request.body(Bytes::from_static(body)).unwrap();
    connector().http(request, 4096).await.unwrap()
}

fn ok(body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status: 200,
        body: body.into(),
    }
}

#[tokio::test(start_paused = true)]
async fn http_requests_are_answered_by_method_and_path_pattern_with_replies_computed_from_them() {
    let by_client_id = PathPattern::template("/orders/by-client/{client_id}").unwrap();
    let order = |req: &HttpRequest| {
        let id = req.param("client_id").unwrap();
        ok(format!(
            "order|client_id={id}|q={}",
            req.query.as_deref().unwrap_or("-")
        ))
    };
    let auth = |req: &HttpRequest| {
        let key = req.header("x-api-key").unwrap_or("none");
        let body = String::from_utf8_lossy(&req.body);
        ok(format!("auth_ack|key={key}|body={body}"))
    };
    let router = HttpRouter::new()
        .route_fn(Method::GET, by_client_id, order)
        .route_fn(Method::POST, PathPattern::exact("/auth"), auth)
        .route(
            Method::DELETE,
            PathPattern::prefix("/orders"),
            ok("cancelled"),
        )
        .route(Method::GET, PathPattern::prefix("/orders/"), ok("list"));
    let server = StubServer::start(WsScript::default(), router)
        .await
        .unwrap();

    let found = call(&server, Method::GET, "/orders/by-client/c-42?x=1", &[], b"").await;
    assert_eq!(found.status(), StatusCode::OK);
    assert_eq!(found.body().as_ref(), b"order|client_id=c-42|q=x=1");
    let other = call(&server, Method::GET, "/orders/by-client/7", &[], b"").await;
    assert_eq!(other.body().as_ref(), b"order|client_id=7|q=-");
    // The body and headers reach the function.
    let key = [("X-Api-Key", "SYNTHETIC-1")];
    let acked = call(&server, Method::POST, "/auth", &key, b"{\"n\":1}").await;
    assert_eq!(
        acked.body().as_ref(),
        b"auth_ack|key=SYNTHETIC-1|body={\"n\":1}"
    );
    // A prefix matches itself and anything below it, on a segment boundary.
    let cancel_all = call(&server, Method::DELETE, "/orders", &[], b"").await;
    assert_eq!(cancel_all.body().as_ref(), b"cancelled");
    let cancel_one = call(&server, Method::DELETE, "/orders/9", &[], b"").await;
    assert_eq!(cancel_one.body().as_ref(), b"cancelled");
    let not_below = call(&server, Method::DELETE, "/ordersx", &[], b"").await;
    assert_eq!(not_below.status(), StatusCode::NOT_FOUND);
    // A route answers its method only; rules are tried in the order they were added, so the
    // template wins over the later GET prefix, which takes what the template does not match.
    let wrong_method = call(&server, Method::PUT, "/auth", &[], b"").await;
    assert_eq!(wrong_method.status(), StatusCode::NOT_FOUND);
    let listed = call(&server, Method::GET, "/orders/by-client", &[], b"").await;
    assert_eq!(listed.body().as_ref(), b"list");
    let deeper = call(&server, Method::GET, "/orders/by-client/7/fills", &[], b"").await;
    assert_eq!(deeper.body().as_ref(), b"list");
    assert_eq!(
        server.http_requests(),
        [
            "GET /orders/by-client/c-42?x=1",
            "GET /orders/by-client/7",
            "POST /auth",
            "DELETE /orders",
            "DELETE /orders/9",
            "DELETE /ordersx",
            "PUT /auth",
            "GET /orders/by-client",
            "GET /orders/by-client/7/fills",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_fixed_route_map_still_answers_any_method_and_rules_follow_it() {
    let routes = fbc_conformance::HttpRoutes::from([("/markets".to_owned(), ok("[1]"))]);
    let router = HttpRouter::from(routes).route(Method::GET, PathPattern::prefix("/"), ok("any"));
    let server = StubServer::start(WsScript::default(), router)
        .await
        .unwrap();
    let posted = call(&server, Method::POST, "/markets", &[], b"x").await;
    assert_eq!(posted.body().as_ref(), b"[1]");
    let rest = call(&server, Method::GET, "/elsewhere/deep", &[], b"").await;
    assert_eq!(rest.body().as_ref(), b"any");
    let root = call(&server, Method::GET, "/", &[], b"").await;
    assert_eq!(root.body().as_ref(), b"any");
}

#[tokio::test(start_paused = true)]
async fn a_chunked_request_or_an_unknown_method_token_is_refused() {
    let router =
        HttpRouter::new().route_fn(Method::POST, PathPattern::prefix("/"), |r: &HttpRequest| {
            ok(r.body.clone())
        });
    let server = StubServer::start(WsScript::default(), router)
        .await
        .unwrap();
    let addr = server.http_url("").trim_start_matches("http://").to_owned();
    // The stub reads bodies by Content-Length only, so a chunked body is refused rather than
    // handed to a function as empty; a method that is not a token cannot be matched.
    for request in [
        "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
        "P(ST /x HTTP/1.1\r\n\r\n",
    ] {
        let mut raw = TcpStream::connect(&addr).await.unwrap();
        raw.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        raw.read_to_string(&mut answer).await.unwrap();
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }
    // A body read past its stated length in one read is cut to that length.
    let mut raw = TcpStream::connect(&addr).await.unwrap();
    let request = "POST /x HTTP/1.1\r\nContent-Length: 2\r\n\r\nabcdef";
    raw.write_all(request.as_bytes()).await.unwrap();
    let mut answer = String::new();
    raw.read_to_string(&mut answer).await.unwrap();
    assert!(
        answer.starts_with("HTTP/1.1 200 ") && answer.ends_with("\r\n\r\nab"),
        "{answer}"
    );
    assert_eq!(server.http_requests(), ["POST /x"]);
}

#[test]
fn a_template_names_each_variable_segment_once_and_refuses_what_it_cannot_match() {
    let pattern = PathPattern::template("/a/{x}/b/{y}").unwrap();
    assert_eq!(
        pattern.matches("/a/1/b/2"),
        Some(vec![
            ("x".to_owned(), "1".to_owned()),
            ("y".to_owned(), "2".to_owned())
        ])
    );
    // A variable segment is one non-empty segment; the template matches whole paths only.
    assert_eq!(pattern.matches("/a//b/2"), None);
    assert_eq!(pattern.matches("/a/1/b"), None);
    assert_eq!(pattern.matches("/a/1/b/2/c"), None);
    assert_eq!(pattern.matches("/a/1/c/2"), None);
    assert_eq!(PathPattern::exact("/a").matches("/a"), Some(Vec::new()));
    assert_eq!(PathPattern::exact("/a").matches("/a/"), None);
    assert_eq!(PathPattern::prefix("/a/").matches("/a/b"), Some(Vec::new()));
    assert_eq!(PathPattern::prefix("/a/").matches("/a"), None);
    for (bad, err) in [
        ("orders", PatternError::NotAbsolute),
        ("/a/{}", PatternError::Segment("{}".to_owned())),
        ("/a/{x", PatternError::Segment("{x".to_owned())),
        ("/a/x}", PatternError::Segment("x}".to_owned())),
        ("/a/p{x}", PatternError::Segment("p{x}".to_owned())),
        ("/a/{x}/{x}", PatternError::Repeated("x".to_owned())),
    ] {
        assert_eq!(PathPattern::template(bad).unwrap_err(), err, "{bad}");
    }
    assert_eq!(
        PatternError::NotAbsolute.to_string(),
        "a path template starts with /"
    );
    assert_eq!(
        PatternError::Segment("{x".to_owned()).to_string(),
        "segment {x is neither literal nor one {name}"
    );
    assert_eq!(
        PatternError::Repeated("x".to_owned()).to_string(),
        "variable x is named twice"
    );
}
