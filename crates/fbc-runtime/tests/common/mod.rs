//! Local servers the runtime's integration tests run against, each on a 127.0.0.1 ephemeral
//! port: a SOCKS5 stub that records every CONNECT target and resolves host names from its own
//! table, a WebSocket echo server, and an HTTP/1.1 server that answers with the request line it
//! saw, each of the two servers plain or behind TLS with a certificate from a CA the test
//! generates ([`tls`]). Nothing here reaches the internet. Later runtime tickets reuse and extend
//! these only as their own done lines need.

#![allow(dead_code)]

pub mod tls;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use tls::TlsServer;

const LOCAL: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

async fn listen() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind((LOCAL, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

/// A CONNECT request as the stub received it: the address type byte (3 is DOMAINNAME) and the
/// host and port it named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectTarget {
    pub atyp: u8,
    pub host: String,
    pub port: u16,
}

/// What the stub answers to a CONNECT.
#[derive(Debug, Clone, Copy)]
pub enum Answer {
    /// Resolve the name from the stub's table and relay; an unknown name is refused with
    /// reply code 4 (host unreachable).
    Relay,
    /// Refuse every CONNECT with this reply code.
    Refuse(u8),
}

/// A SOCKS5 proxy (RFC 1928, no-authentication method only) on a 127.0.0.1 ephemeral port.
pub struct Socks5Stub {
    pub addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    targets: Arc<Mutex<Vec<ConnectTarget>>>,
}

impl Socks5Stub {
    /// A stub that resolves the names in `names` (and only those) to their addresses.
    pub async fn start(names: &[(&str, IpAddr)], answer: Answer) -> Self {
        let (listener, addr) = listen().await;
        let names: Arc<HashMap<String, IpAddr>> =
            Arc::new(names.iter().map(|(n, ip)| (n.to_string(), *ip)).collect());
        let connections = Arc::new(AtomicUsize::new(0));
        let targets = Arc::new(Mutex::new(Vec::new()));
        let (count, log) = (connections.clone(), targets.clone());
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                count.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve_socks(stream, names.clone(), answer, log.clone()));
            }
        });
        Socks5Stub {
            addr,
            connections,
            targets,
        }
    }

    /// The TCP connections the stub accepted.
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    /// Every CONNECT target, in the order the requests arrived.
    pub fn targets(&self) -> Vec<ConnectTarget> {
        self.targets.lock().unwrap().clone()
    }
}

async fn serve_socks(
    mut client: TcpStream,
    names: Arc<HashMap<String, IpAddr>>,
    answer: Answer,
    log: Arc<Mutex<Vec<ConnectTarget>>>,
) {
    let mut head = [0u8; 2];
    client.read_exact(&mut head).await.unwrap();
    let mut methods = vec![0u8; head[1] as usize];
    client.read_exact(&mut methods).await.unwrap();
    assert_eq!(head[0], 5, "greeting version");
    assert_eq!(methods, [0], "the client offers no-authentication only");
    client.write_all(&[5, 0]).await.unwrap();

    let mut request = [0u8; 4];
    client.read_exact(&mut request).await.unwrap();
    assert_eq!(request[..3], [5, 1, 0], "a CONNECT request");
    let atyp = request[3];
    let host = match atyp {
        1 => {
            let mut ip = [0u8; 4];
            client.read_exact(&mut ip).await.unwrap();
            Ipv4Addr::from(ip).to_string()
        }
        3 => {
            let len = client.read_u8().await.unwrap();
            let mut name = vec![0u8; len as usize];
            client.read_exact(&mut name).await.unwrap();
            String::from_utf8(name).unwrap()
        }
        other => panic!("the stub does not take address type {other}"),
    };
    let port = client.read_u16().await.unwrap();
    log.lock().unwrap().push(ConnectTarget {
        atyp,
        host: host.clone(),
        port,
    });

    let resolved = match atyp {
        3 => names.get(&host).copied(),
        _ => host.parse().ok(),
    };
    let code = match (answer, resolved) {
        (Answer::Refuse(code), _) => code,
        (Answer::Relay, None) => 4,
        (Answer::Relay, Some(_)) => 0,
    };
    if code != 0 {
        client
            .write_all(&[5, code, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        return;
    }
    let mut upstream = TcpStream::connect((resolved.unwrap(), port)).await.unwrap();
    client
        .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
        .await
        .unwrap();
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
}

/// A WebSocket server that echoes every text and binary message.
pub struct WsServer {
    pub addr: SocketAddr,
    connections: Arc<AtomicUsize>,
}

impl WsServer {
    pub async fn start() -> Self {
        Self::run(None).await
    }

    /// The same server behind TLS (`wss://`).
    pub async fn start_tls(tls: TlsServer) -> Self {
        Self::run(Some(tls)).await
    }

    async fn run(tls: Option<TlsServer>) -> Self {
        let (listener, addr) = listen().await;
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                count.fetch_add(1, Ordering::SeqCst);
                let tls = tls.clone();
                tokio::spawn(async move {
                    match tls {
                        None => serve_ws(stream).await,
                        Some(tls) => {
                            if let Some(stream) = tls.accept(stream).await {
                                serve_ws(stream).await;
                            }
                        }
                    }
                });
            }
        });
        WsServer { addr, connections }
    }

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

async fn serve_ws<S: AsyncRead + AsyncWrite + Unpin>(stream: S) {
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    while let Some(Ok(message)) = ws.next().await {
        if message.is_text() || message.is_binary() {
            ws.send(message).await.unwrap();
        }
    }
}

/// A request as the HTTP server read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenRequest {
    /// The request line's method and target, e.g. `GET /v1/markets?market=X`.
    pub line: String,
    /// The Host header.
    pub host: String,
    pub body: Vec<u8>,
}

/// An HTTP/1.1 server that answers every request with 200 and a body that is its request line,
/// then closes the connection. `silent()` instead closes every connection without answering.
pub struct HttpServer {
    pub addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<SeenRequest>>>,
}

impl HttpServer {
    pub async fn start() -> Self {
        Self::run(true, None).await
    }

    /// The same server behind TLS (`https://`).
    pub async fn start_tls(tls: TlsServer) -> Self {
        Self::run(true, Some(tls)).await
    }

    pub async fn silent() -> Self {
        Self::run(false, None).await
    }

    async fn run(answers: bool, tls: Option<TlsServer>) -> Self {
        let (listener, addr) = listen().await;
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (count, log) = (connections.clone(), requests.clone());
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                count.fetch_add(1, Ordering::SeqCst);
                if !answers {
                    continue;
                }
                let (log, tls) = (log.clone(), tls.clone());
                tokio::spawn(async move {
                    match tls {
                        None => serve_http(stream, log).await,
                        Some(tls) => {
                            if let Some(stream) = tls.accept(stream).await {
                                serve_http(stream, log).await;
                            }
                        }
                    }
                });
            }
        });
        HttpServer {
            addr,
            connections,
            requests,
        }
    }

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    pub fn requests(&self) -> Vec<SeenRequest> {
        self.requests.lock().unwrap().clone()
    }
}

async fn serve_http<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    log: Arc<Mutex<Vec<SeenRequest>>>,
) {
    let mut seen = Vec::new();
    let head_end = loop {
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "the client closed before the request head ended");
        seen.extend_from_slice(&chunk[..n]);
        if let Some(i) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8(seen[..head_end].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap();
    let line = request_line.rsplit_once(' ').unwrap().0.to_owned();
    let header = |name: &str| {
        head.split("\r\n")
            .find_map(|l| {
                l.split_once(':')
                    .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            })
            .map(|(_, v)| v.trim().to_owned())
    };
    let length: usize = header("content-length").map_or(0, |v| v.parse().unwrap());
    let mut body = seen[head_end..].to_vec();
    while body.len() < length {
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk).await.unwrap();
        body.extend_from_slice(&chunk[..n]);
    }
    log.lock().unwrap().push(SeenRequest {
        line: line.clone(),
        host: header("host").unwrap_or_default(),
        body,
    });
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{line}",
        line.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
    let _ = stream.shutdown().await;
}

/// A server on a 127.0.0.1 ephemeral port that answers each connection with `reply` and
/// closes it, whatever it received; for a proxy that speaks a broken SOCKS5.
pub async fn scripted(reply: &'static [u8]) -> SocketAddr {
    let (listener, addr) = listen().await;
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf).await;
            let _ = stream.write_all(reply).await;
        }
    });
    addr
}

/// A 127.0.0.1 port nothing listens on (bound, then released).
pub async fn closed_port() -> u16 {
    let (listener, addr) = listen().await;
    drop(listener);
    addr.port()
}
