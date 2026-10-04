//! FBC-27a: TLS (`wss://`, `https://`) runs over the one connector, so the SOCKS5 proxy applies
//! to encrypted traffic exactly as to plain (0002). The servers present certificates from a CA
//! the test generates in memory and adds as a trust anchor; nothing reaches the internet.

mod common;

use std::net::{IpAddr, Ipv4Addr};

use common::tls::TestCa;
use common::{Answer, ConnectTarget, HttpServer, Socks5Stub, WsServer, scripted};
use fbc_runtime::http::{Bytes, Request};
use fbc_runtime::ws::Message;
use fbc_runtime::{Cause, Connector, NetError, ProxyConfig, Step};
use futures_util::{SinkExt, StreamExt};

const LOCAL: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const DOMAINNAME: u8 = 3;
const LIMIT: usize = 1 << 16;
/// The names the servers' certificates carry: two only the stub resolves, and one the process
/// resolves itself for the direct path.
const NAMES: &[&str] = &["ws.venue.test", "api.venue.test", "localhost"];

fn trusting(proxy: ProxyConfig, ca: &TestCa) -> Connector {
    let mut connector = Connector::new(proxy);
    connector.add_trust_anchor(&ca.der()).unwrap();
    connector
}

fn socks5(stub: &Socks5Stub) -> ProxyConfig {
    ProxyConfig::Socks5 {
        host: stub.addr.ip().to_string(),
        port: stub.addr.port(),
    }
}

async fn venue_stub() -> Socks5Stub {
    Socks5Stub::start(
        &[("ws.venue.test", LOCAL), ("api.venue.test", LOCAL)],
        Answer::Relay,
    )
    .await
}

fn get(url: &str) -> Request<Bytes> {
    Request::builder().uri(url).body(Bytes::new()).unwrap()
}

async fn echo_once(connector: &Connector, url: &str) {
    let mut ws = connector.websocket(url).await.unwrap();
    ws.send(Message::text("ping")).await.unwrap();
    assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("ping"));
}

fn named(host: &str, port: u16) -> ConnectTarget {
    ConnectTarget {
        atyp: DOMAINNAME,
        host: host.into(),
        port,
    }
}

#[tokio::test]
async fn wss_and_https_pass_through_socks5_with_the_target_hostname_as_sni() {
    let ca = TestCa::new();
    let tls = ca.server(NAMES);
    let ws = WsServer::start_tls(tls.clone()).await;
    let http = HttpServer::start_tls(tls.clone()).await;
    let stub = venue_stub().await;
    let connector = trusting(socks5(&stub), &ca);
    let (ws_port, http_port) = (ws.addr.port(), http.addr.port());

    echo_once(&connector, &format!("wss://ws.venue.test:{ws_port}/stream")).await;
    let url = format!("https://api.venue.test:{http_port}/v1/markets?market=X");
    let response = connector.http(get(&url), LIMIT).await.unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.body().as_ref(), b"GET /v1/markets?market=X");
    assert_eq!(
        stub.targets(),
        [
            named("ws.venue.test", ws_port),
            named("api.venue.test", http_port)
        ]
    );
    assert_eq!(
        tls.server_names(),
        [Some("ws.venue.test".into()), Some("api.venue.test".into())]
    );
    assert_eq!(
        http.requests()[0].host,
        format!("api.venue.test:{http_port}")
    );
}

#[tokio::test]
async fn wss_and_https_reach_their_servers_directly_and_the_stub_sees_no_connection() {
    let ca = TestCa::new();
    let tls = ca.server(NAMES);
    let ws = WsServer::start_tls(tls.clone()).await;
    let http = HttpServer::start_tls(tls.clone()).await;
    let stub = venue_stub().await;
    let connector = trusting(ProxyConfig::Direct, &ca);

    echo_once(&connector, &format!("wss://localhost:{}/", ws.addr.port())).await;
    let url = format!("https://localhost:{}/v1/time", http.addr.port());
    let response = connector.http(get(&url), LIMIT).await.unwrap();

    assert_eq!(response.body().as_ref(), b"GET /v1/time");
    assert_eq!((ws.connections(), http.connections()), (1, 1));
    assert_eq!(
        tls.server_names(),
        [Some("localhost".into()), Some("localhost".into())]
    );
    assert_eq!(stub.connections(), 0);
}

fn assert_tls_refused(err: &NetError, why: &str) {
    assert_eq!(err.step(), Step::TlsHandshake, "{err:?}");
    let shown = format!("{err} {err:?}");
    assert!(
        err.to_string().starts_with("TLS handshake failed: "),
        "{err}"
    );
    assert!(shown.contains(why), "{shown}");
    for secret in ["user", "hunter2", "token", "sesame", "orders"] {
        assert!(!shown.contains(secret), "{shown}");
    }
}

#[tokio::test]
async fn a_certificate_for_another_host_is_refused_at_the_tls_step_directly_and_by_proxy() {
    let ca = TestCa::new();
    let tls = ca.server(&["other.test"]);
    let ws = WsServer::start_tls(tls.clone()).await;
    let http = HttpServer::start_tls(tls.clone()).await;
    let stub = venue_stub().await;
    let secret = "user:hunter2@";
    let query = "/v1/orders?token=sesame";

    for (connector, ws_host, http_host) in [
        (trusting(ProxyConfig::Direct, &ca), "localhost", "localhost"),
        (
            trusting(socks5(&stub), &ca),
            "ws.venue.test",
            "api.venue.test",
        ),
    ] {
        let wss = format!("wss://{secret}{ws_host}:{}{query}", ws.addr.port());
        let https = format!("https://{secret}{http_host}:{}{query}", http.addr.port());
        let ws_err = connector.websocket(&wss).await.unwrap_err();
        let http_err = connector.http(get(&https), LIMIT).await.unwrap_err();
        assert_tls_refused(&ws_err, "not valid for name");
        assert_tls_refused(&http_err, "not valid for name");
    }

    assert_eq!(stub.targets().len(), 2);
    assert!(tls.server_names().is_empty(), "no handshake completed");
    assert!(http.requests().is_empty());
}

#[tokio::test]
async fn without_the_test_ca_as_a_trust_anchor_the_server_is_refused() {
    let ca = TestCa::new();
    let http = HttpServer::start_tls(ca.server(NAMES)).await;
    let url = format!("https://localhost:{}/", http.addr.port());

    let err = Connector::new(ProxyConfig::Direct)
        .http(get(&url), LIMIT)
        .await
        .unwrap_err();

    assert_tls_refused(&err, "UnknownIssuer");
}

#[tokio::test]
async fn a_trust_anchor_that_is_not_a_certificate_is_refused_at_the_trust_step() {
    let mut connector = Connector::new(ProxyConfig::Direct);
    let err = connector
        .add_trust_anchor(b"not a certificate")
        .unwrap_err();
    assert_eq!(err.step(), Step::TlsTrust);
    assert!(
        err.to_string().starts_with("TLS trust anchor failed: "),
        "{err}"
    );
}

#[tokio::test]
async fn a_server_that_does_not_speak_tls_fails_at_the_tls_step() {
    let plain = scripted(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
    let silent = HttpServer::silent().await;
    let connector = Connector::new(ProxyConfig::Direct);

    let garbled = connector
        .http(get(&format!("https://localhost:{}/", plain.port())), LIMIT)
        .await
        .unwrap_err();
    let hung_up = connector
        .websocket(&format!("wss://localhost:{}/", silent.addr.port()))
        .await
        .unwrap_err();

    assert_eq!(garbled.step(), Step::TlsHandshake);
    assert!(matches!(garbled.cause(), Cause::Detail(_)), "{garbled:?}");
    assert_eq!(hung_up.step(), Step::TlsHandshake);
    assert!(matches!(hung_up.cause(), Cause::Io(_)), "{hung_up:?}");
}
