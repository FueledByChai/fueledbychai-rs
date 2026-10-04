//! FBC-2ds: one connector opens every connection, directly or through a SOCKS5 proxy, and the
//! WebSocket client and the HTTP/1.1 call both ride on it, so the proxy the consumer configures
//! applies to both (0002). Every server is local (tests/common); nothing reaches the internet.

mod common;

use std::net::{IpAddr, Ipv4Addr};

use common::{Answer, ConnectTarget, HttpServer, Socks5Stub, WsServer, closed_port, scripted};
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_runtime::ws::Message;
use fbc_runtime::{Cause, Connector, NetError, ProxyConfig, Step};
use futures_util::{SinkExt, StreamExt};

const LOCAL: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const DOMAINNAME: u8 = 3;
/// The response-body limit the tests pass; far above any body the local servers send.
const LIMIT: usize = 1 << 16;

fn through(stub: &Socks5Stub) -> Connector {
    Connector::new(ProxyConfig::Socks5 {
        host: stub.addr.ip().to_string(),
        port: stub.addr.port(),
    })
}

fn get(url: &str) -> Request<Bytes> {
    Request::builder().uri(url).body(Bytes::new()).unwrap()
}

async fn echo_once(connector: &Connector, url: &str) {
    let mut ws = connector.websocket(url).await.unwrap();
    ws.send(Message::text("ping")).await.unwrap();
    let echoed = ws.next().await.unwrap().unwrap();
    assert_eq!(echoed, Message::text("ping"));
}

/// Names only the stub can resolve: if the connector resolved them itself, it would fail.
async fn venue_stub(answer: Answer) -> Socks5Stub {
    Socks5Stub::start(
        &[("ws.venue.test", LOCAL), ("api.venue.test", LOCAL)],
        answer,
    )
    .await
}

#[tokio::test]
async fn socks5_carries_a_websocket_with_the_target_named_as_a_hostname() {
    let server = WsServer::start().await;
    let stub = venue_stub(Answer::Relay).await;
    let port = server.addr.port();

    echo_once(
        &through(&stub),
        &format!("ws://ws.venue.test:{port}/stream?c=1"),
    )
    .await;

    assert_eq!(
        stub.targets(),
        [ConnectTarget {
            atyp: DOMAINNAME,
            host: "ws.venue.test".into(),
            port,
        }]
    );
    assert_eq!(server.connections(), 1);
}

#[tokio::test]
async fn socks5_carries_an_http_request_with_the_target_named_as_a_hostname() {
    let server = HttpServer::start().await;
    let stub = venue_stub(Answer::Relay).await;
    let port = server.addr.port();
    let url = format!("http://api.venue.test:{port}/v1/markets?market=X");

    let response = through(&stub).http(get(&url), LIMIT).await.unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.body().as_ref(), b"GET /v1/markets?market=X");
    assert_eq!(
        stub.targets(),
        [ConnectTarget {
            atyp: DOMAINNAME,
            host: "api.venue.test".into(),
            port,
        }]
    );
    let seen = server.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].host, format!("api.venue.test:{port}"));
}

#[tokio::test]
async fn direct_reaches_both_servers_and_the_stub_sees_no_connection() {
    let ws = WsServer::start().await;
    let http = HttpServer::start().await;
    let stub = venue_stub(Answer::Relay).await;
    let direct = Connector::new(ProxyConfig::Direct);

    echo_once(&direct, &format!("ws://{}/stream", ws.addr)).await;
    let url = format!("http://{}/v1/time", http.addr);
    let response = direct.http(get(&url), LIMIT).await.unwrap();

    assert_eq!(response.body().as_ref(), b"GET /v1/time");
    assert_eq!((ws.connections(), http.connections()), (1, 1));
    assert_eq!(stub.connections(), 0);
    assert!(stub.targets().is_empty());
}

fn assert_refused(err: &NetError, code: u8, name: &str) {
    assert_eq!(err.step(), Step::ProxyConnect);
    assert_eq!(err.cause(), &Cause::Reply(code));
    let text = err.to_string();
    assert!(text.contains("SOCKS5 CONNECT"), "{text}");
    assert!(
        text.contains(&format!("reply code 0x{code:02x} ({name})")),
        "{text}"
    );
}

#[tokio::test]
async fn a_refused_connect_names_the_proxy_step_and_the_reply_code() {
    let stub = venue_stub(Answer::Refuse(5)).await;
    let connector = through(&stub);

    let ws = connector.websocket("ws://ws.venue.test:9/stream").await;
    let http = connector
        .http(get("http://api.venue.test:9/v1/time"), LIMIT)
        .await;

    assert_refused(&ws.unwrap_err(), 5, "connection refused");
    assert_refused(&http.unwrap_err(), 5, "connection refused");
    assert_eq!(stub.targets().len(), 2);
}

#[tokio::test]
async fn a_name_the_proxy_cannot_resolve_is_host_unreachable() {
    let stub = venue_stub(Answer::Relay).await;
    let err = through(&stub)
        .websocket("ws://elsewhere.test:9/")
        .await
        .unwrap_err();
    assert_refused(&err, 4, "host unreachable");
}

#[tokio::test]
async fn an_ip_literal_goes_to_the_proxy_as_an_ipv4_address() {
    let server = HttpServer::start().await;
    let stub = venue_stub(Answer::Relay).await;

    let url = format!("http://{}/v1/time", server.addr);
    through(&stub).http(get(&url), LIMIT).await.unwrap();

    let targets = stub.targets();
    assert_eq!(
        (targets[0].atyp, targets[0].host.as_str()),
        (1, "127.0.0.1")
    );
}

#[tokio::test]
async fn a_proxy_that_is_not_listening_fails_at_the_proxy_tcp_step() {
    let connector = Connector::new(ProxyConfig::Socks5 {
        host: "127.0.0.1".into(),
        port: closed_port().await,
    });
    let err = connector
        .websocket("ws://ws.venue.test:9/")
        .await
        .unwrap_err();
    assert_eq!(err.step(), Step::ProxyTcp);
    assert_eq!(
        err.cause(),
        &Cause::Io(std::io::ErrorKind::ConnectionRefused)
    );
}

#[tokio::test]
async fn a_proxy_that_wants_authentication_fails_at_the_greeting() {
    let proxy = scripted(&[5, 0xff]).await;
    let connector = Connector::new(ProxyConfig::Socks5 {
        host: proxy.ip().to_string(),
        port: proxy.port(),
    });
    let err = connector
        .http(get("http://api.venue.test/"), LIMIT)
        .await
        .unwrap_err();
    assert_eq!(err.step(), Step::ProxyGreeting);
    assert!(
        err.to_string().starts_with("SOCKS5 greeting failed: "),
        "{err}"
    );
}

#[tokio::test]
async fn a_closed_target_fails_at_the_target_tcp_step_without_quoting_the_url() {
    let port = closed_port().await;
    let direct = Connector::new(ProxyConfig::Direct);
    let url = format!("http://user:hunter2@127.0.0.1:{port}/v1/orders?token=sesame");

    let http = direct.http(get(&url), LIMIT).await.unwrap_err();
    let ws = direct
        .websocket(&url.replacen("http", "ws", 1))
        .await
        .unwrap_err();

    for err in [http, ws] {
        assert_eq!(err.step(), Step::TargetTcp);
        let shown = format!("{err} {err:?}");
        for secret in ["user", "hunter2", "token", "sesame", "orders"] {
            assert!(!shown.contains(secret), "{shown}");
        }
    }
}

#[tokio::test]
async fn foreign_schemes_fail_at_the_url_step_before_any_connection() {
    let stub = venue_stub(Answer::Relay).await;
    let connector = through(&stub);

    let cases = [
        connector.websocket("https://ws.venue.test/").await,
        connector.websocket("http://ws.venue.test/").await,
        connector.websocket("not a url").await,
    ];
    for result in cases {
        assert_eq!(result.unwrap_err().step(), Step::Url);
    }
    for url in ["wss://api.venue.test/", "ws://api.venue.test/", "/relative"] {
        let err = connector.http(get(url), LIMIT).await.unwrap_err();
        assert_eq!(err.step(), Step::Url);
    }
    assert_eq!(stub.connections(), 0);
}

#[tokio::test]
async fn a_server_that_will_not_upgrade_fails_at_the_websocket_step_with_its_status() {
    let server = HttpServer::start().await;
    let direct = Connector::new(ProxyConfig::Direct);
    let err = direct
        .websocket(&format!("ws://{}/stream", server.addr))
        .await
        .unwrap_err();
    assert_eq!(err.step(), Step::WebSocketUpgrade);
    assert_eq!(err.cause(), &Cause::Status(200));
}

#[tokio::test]
async fn a_server_that_hangs_up_fails_at_the_websocket_or_http_step() {
    let server = HttpServer::silent().await;
    let direct = Connector::new(ProxyConfig::Direct);

    let ws = direct.websocket(&format!("ws://{}/", server.addr)).await;
    let http = direct
        .http(get(&format!("http://{}/", server.addr)), LIMIT)
        .await;

    assert_eq!(ws.unwrap_err().step(), Step::WebSocketUpgrade);
    let err = http.unwrap_err();
    assert_eq!(err.step(), Step::Http);
    assert!(
        err.to_string().starts_with("HTTP/1.1 request failed: "),
        "{err}"
    );
}

#[tokio::test]
async fn a_request_body_and_a_caller_host_header_reach_the_server() {
    let server = HttpServer::start().await;
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{}/v1/orders", server.addr))
        .header("host", "api.venue.test")
        .body(Bytes::from_static(b"{\"x\":1}"))
        .unwrap();

    let response = Connector::new(ProxyConfig::Direct)
        .http(request, LIMIT)
        .await
        .unwrap();

    assert_eq!(response.body().as_ref(), b"POST /v1/orders");
    let seen = server.requests();
    assert_eq!(seen[0].host, "api.venue.test");
    assert_eq!(seen[0].body, b"{\"x\":1}");
}

#[tokio::test]
async fn a_response_body_over_the_callers_limit_fails_at_the_http_step() {
    let server = HttpServer::start().await;
    let direct = Connector::new(ProxyConfig::Direct);
    let url = format!("http://{}/v1/time", server.addr);
    let body = b"GET /v1/time".len();

    let fits = direct.http(get(&url), body).await.unwrap();
    let over = direct.http(get(&url), body - 1).await.unwrap_err();

    assert_eq!(fits.body().len(), body);
    assert_eq!(over.step(), Step::Http);
    assert_eq!(
        over.cause(),
        &Cause::Protocol("the response body is over the caller's limit")
    );
}

#[tokio::test]
async fn a_body_cut_short_fails_at_the_http_step() {
    let server = scripted(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort").await;
    let err = Connector::new(ProxyConfig::Direct)
        .http(get(&format!("http://{server}/")), LIMIT)
        .await
        .unwrap_err();
    assert_eq!(err.step(), Step::Http);
    assert!(matches!(err.cause(), Cause::Detail(_)), "{err:?}");
}
