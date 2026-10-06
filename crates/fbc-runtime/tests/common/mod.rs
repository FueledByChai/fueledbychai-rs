//! Local servers the runtime's integration tests run against, each on a 127.0.0.1 ephemeral
//! port: a SOCKS5 stub that records every CONNECT target and resolves host names from its own
//! table, a WebSocket echo server, and an HTTP/1.1 server that answers with the request line it
//! saw, each of the two servers plain or behind TLS with a certificate from a CA the test
//! generates ([`tls`]); a WebSocket server each test scripts connection by connection
//! ([`ScriptedWs`], plain or behind TLS), one that refuses every connection and reports when ([`refusing`]), an HTTP
//! server whose answers the test scripts request by request ([`ScriptedHttp`]), and a toy
//! market-data venue ([`toy`]), and a WebSocket server whose upgrade response the test writes
//! ([`upgrading`]). Nothing here reaches the internet. Later runtime tickets reuse and extend
//! these only as their own done lines need.

#![allow(dead_code)]

pub mod tls;
pub mod toy;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};

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

/// A WebSocket server whose connections the test drives one by one, each as a [`Peer`].
pub struct ScriptedWs {
    pub addr: SocketAddr,
    peers: mpsc::UnboundedReceiver<Peer>,
}

/// What a [`Peer`] asks its connection to do.
enum Out {
    Send(Message),
    SendAll(Vec<Message>),
    Drop,
    Stall(Option<std::time::Duration>),
    Hold(oneshot::Receiver<()>),
}

/// What a [`Peer`] hears when the client sends a WebSocket ping.
pub const PING: &str = "<ping>";
/// What a [`Peer`] hears when the client answers its ping.
pub const PONG: &str = "<pong>";

/// What a [`Peer`] hears when the client sends a binary frame of `bytes`: `<binary HEX>`, the
/// bytes in lower-case hex.
pub fn heard_binary(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("<binary {hex}>")
}

/// One accepted connection: the text frames (and pings) the client sent, in order, each with
/// the (tokio) instant the server read it, and a way to answer.
pub struct Peer {
    from_client: mpsc::UnboundedReceiver<(Instant, String)>,
    to_client: mpsc::UnboundedSender<Out>,
}

impl ScriptedWs {
    pub async fn start() -> Self {
        Self::run(None).await
    }

    /// The same server behind TLS (`wss://`, [`ScriptedWs::wss_url`]).
    pub async fn start_tls(tls: TlsServer) -> Self {
        Self::run(Some(tls)).await
    }

    async fn run(tls: Option<TlsServer>) -> Self {
        let (listener, addr) = listen().await;
        let (tx, peers) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                // Each frame the script sends leaves at once: under Nagle, Linux holds a small
                // frame behind the previous one's ACK for wall time, so the session reads it
                // only after the test has moved the paused clock.
                stream.set_nodelay(true).unwrap();
                let (heard, from_client) = mpsc::unbounded_channel();
                let (to_client, out) = mpsc::unbounded_channel();
                let _ = tx.send(Peer {
                    from_client,
                    to_client,
                });
                let tls = tls.clone();
                tokio::spawn(async move {
                    match tls {
                        None => script_ws(stream, heard, out).await,
                        Some(tls) => {
                            if let Some(stream) = tls.accept(stream).await {
                                script_ws(stream, heard, out).await;
                            }
                        }
                    }
                });
            }
        });
        ScriptedWs { addr, peers }
    }

    /// The `wss://` URL of a server started with [`ScriptedWs::start_tls`], by the name its
    /// certificate gives 127.0.0.1.
    pub fn wss_url(&self, host: &str) -> String {
        format!("wss://{host}:{}/md", self.addr.port())
    }

    pub fn url(&self) -> String {
        format!("ws://{}/md", self.addr)
    }

    /// The next connection the server accepted.
    pub async fn accept(&mut self) -> Peer {
        self.peers.recv().await.unwrap()
    }

    /// A connection the server has already accepted, if any.
    pub fn try_accept(&mut self) -> Option<Peer> {
        self.peers.try_recv().ok()
    }
}

/// Plays one scripted connection: reports each text or binary frame the client sends on `heard`
/// and does what the test asks on `out`.
async fn script_ws<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    heard: mpsc::UnboundedSender<(Instant, String)>,
    mut out: mpsc::UnboundedReceiver<Out>,
) {
    // No size limit: a test may stall the reads under a frame larger than the socket buffers
    // hold, then resume them.
    let config = WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);
    let ws = tokio_tungstenite::accept_async_with_config(stream, Some(config));
    let mut ws = ws.await.unwrap();
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let _ = heard.send((Instant::now(), text.as_str().to_owned()));
                }
                // FBC-djl: a WebSocket ping is heard as `<ping>`, a pong as `<pong>`.
                Some(Ok(Message::Ping(_))) => {
                    let _ = heard.send((Instant::now(), PING.to_owned()));
                }
                Some(Ok(Message::Pong(_))) => {
                    let _ = heard.send((Instant::now(), PONG.to_owned()));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    let _ = heard.send((Instant::now(), heard_binary(&bytes)));
                }
                Some(Ok(_)) => {}
                _ => break,
            },
            cmd = out.recv() => match cmd {
                Some(Out::Send(message)) => ws.send(message).await.unwrap(),
                // A client that closes mid-flood ends the connection, as a drop does.
                Some(Out::SendAll(messages)) => {
                    for message in messages {
                        if ws.feed(message).await.is_err() {
                            break;
                        }
                    }
                    if ws.flush().await.is_err() {
                        break;
                    }
                }
                Some(Out::Stall(None)) => std::future::pending().await,
                Some(Out::Stall(Some(pause))) => tokio::time::sleep(pause).await,
                Some(Out::Hold(release)) => {
                    let _ = release.await;
                }
                _ => {
                    let _ = ws.close(None).await;
                    break;
                }
            },
        }
    }
}

impl Peer {
    /// The next text frame from the client, or `None` once the connection closed.
    pub async fn next(&mut self) -> Option<String> {
        self.next_at().await.map(|(_, text)| text)
    }

    /// The next text frame from the client and the instant the server read it, or `None` once
    /// the connection closed.
    pub async fn next_at(&mut self) -> Option<(Instant, String)> {
        self.from_client.recv().await
    }

    /// Whether the client has sent nothing more yet and the connection is still open.
    pub fn quiet(&mut self) -> bool {
        matches!(
            self.from_client.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        )
    }

    /// The next text frame from the client, which must come.
    pub async fn recv(&mut self) -> String {
        self.next().await.expect("the client closed the connection")
    }

    pub fn send(&self, text: &str) {
        let _ = self.to_client.send(Out::Send(Message::text(text)));
    }

    /// Sends every text of `texts`, buffered and flushed together, so the client finds them
    /// waiting at once rather than one by one.
    pub fn send_all(&self, texts: impl IntoIterator<Item = String>) {
        let messages = texts.into_iter().map(Message::text).collect();
        let _ = self.to_client.send(Out::SendAll(messages));
    }

    pub fn send_binary(&self, bytes: &[u8]) {
        let _ = self
            .to_client
            .send(Out::Send(Message::binary(bytes.to_vec())));
    }

    /// Sends a ping, which carries no data.
    pub fn ping(&self) {
        let _ = self
            .to_client
            .send(Out::Send(Message::Ping(Vec::new().into())));
    }

    /// Sends a ping carrying `payload`.
    pub fn ping_with(&self, payload: &[u8]) {
        let ping = Message::Ping(payload.to_vec().into());
        let _ = self.to_client.send(Out::Send(ping));
    }

    /// Sends a pong carrying `payload`, which answers no ping.
    pub fn pong(&self, payload: &[u8]) {
        let pong = Message::Pong(payload.to_vec().into());
        let _ = self.to_client.send(Out::Send(pong));
    }

    /// Sends a close frame with status `code` and `reason`; the connection closes once the
    /// client answers it.
    pub fn close_with(&self, code: u16, reason: &str) {
        let frame = CloseFrame {
            code: code.into(),
            reason: reason.into(),
        };
        let _ = self.to_client.send(Out::Send(Message::Close(Some(frame))));
    }

    /// Stops reading from the client, holding the connection open.
    pub fn stall(&self) {
        let _ = self.to_client.send(Out::Stall(None));
    }

    /// Stops reading from the client for `pause`, then reads again.
    pub fn stall_for(&self, pause: std::time::Duration) {
        let _ = self.to_client.send(Out::Stall(Some(pause)));
    }

    /// Stops reading from the client until the returned sender fires or drops, then reads
    /// again: the test, not a clock, ends the stall.
    pub fn hold(&self) -> oneshot::Sender<()> {
        let (release, held) = oneshot::channel();
        let _ = self.to_client.send(Out::Hold(held));
        release
    }

    /// Closes the connection.
    pub fn drop_conn(&self) {
        let _ = self.to_client.send(Out::Drop);
    }
}

/// A server on a 127.0.0.1 ephemeral port that accepts connections and never answers on them,
/// reporting the (tokio) instant of each accept.
pub async fn hanging() -> (SocketAddr, mpsc::UnboundedReceiver<Instant>) {
    let (listener, addr) = listen().await;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            held.push(listener.accept().await.unwrap().0);
            let _ = tx.send(Instant::now());
        }
    });
    (addr, rx)
}

/// A server on a 127.0.0.1 ephemeral port whose first connection is a WebSocket that is sent
/// `texts` and then held open, and whose later connections are accepted and never answered;
/// reports the (tokio) instant of each later accept.
pub async fn once_then_hanging(
    texts: &'static [&'static str],
) -> (SocketAddr, mpsc::UnboundedReceiver<Instant>) {
    let (listener, addr) = listen().await;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            for text in texts {
                ws.send(Message::text(*text)).await.unwrap();
            }
            while let Some(Ok(_)) = ws.next().await {}
        });
        let mut held = Vec::new();
        loop {
            held.push(listener.accept().await.unwrap().0);
            let _ = tx.send(Instant::now());
        }
    });
    (addr, rx)
}

/// A server on a 127.0.0.1 ephemeral port that accepts every connection and closes it at once,
/// reporting the (tokio) instant of each accept.
pub async fn refusing() -> (SocketAddr, mpsc::UnboundedReceiver<Instant>) {
    let (listener, addr) = listen().await;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = tx.send(Instant::now());
            drop(stream);
        }
    });
    (addr, rx)
}

/// An HTTP/1.1 server whose requests the test answers one by one, each as an [`Exchange`].
pub struct ScriptedHttp {
    pub addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    exchanges: mpsc::UnboundedReceiver<Exchange>,
}

/// One request the server read, waiting for the test: answer it, hold it unanswered, or drop it,
/// which closes the connection without an answer.
pub struct Exchange {
    /// The request line's method and target, e.g. `GET /poll?syms=A`.
    pub line: String,
    reply: oneshot::Sender<(String, oneshot::Sender<()>)>,
}

impl Exchange {
    /// Answers with `head` (status line and headers, without the final blank line) and `body`,
    /// and returns once the client has closed the connection.
    pub async fn answer(self, head: &str, body: &str) {
        let response = format!(
            "{head}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let (done, closed) = oneshot::channel();
        let _ = self.reply.send((response, done));
        let _ = closed.await;
    }
}

impl ScriptedHttp {
    pub async fn start() -> Self {
        let (listener, addr) = listen().await;
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let (tx, exchanges) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                count.fetch_add(1, Ordering::SeqCst);
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut seen = Vec::new();
                    while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut chunk = [0u8; 1024];
                        match stream.read(&mut chunk).await {
                            Ok(n) if n > 0 => seen.extend_from_slice(&chunk[..n]),
                            _ => return,
                        }
                    }
                    let head = String::from_utf8_lossy(&seen);
                    let line = head.split("\r\n").next().unwrap_or_default();
                    let line = line.rsplit_once(' ').map_or(line, |(l, _)| l).to_owned();
                    let (reply, answer) = oneshot::channel();
                    let _ = tx.send(Exchange { line, reply });
                    if let Ok((response, done)) = answer.await {
                        let _ = stream.write_all(response.as_bytes()).await;
                        let mut rest = [0u8; 1024];
                        while let Ok(n) = stream.read(&mut rest).await {
                            if n == 0 {
                                break;
                            }
                        }
                        let _ = done.send(());
                    }
                });
            }
        });
        ScriptedHttp {
            addr,
            connections,
            exchanges,
        }
    }

    /// `http://<addr><path>`.
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// Connections accepted so far.
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    /// The next request the server read.
    pub async fn request(&mut self) -> Exchange {
        self.exchanges.recv().await.unwrap()
    }

    /// A request the server read that the test has not taken yet.
    pub fn try_request(&mut self) -> Option<Exchange> {
        self.exchanges.try_recv().ok()
    }
}

/// A WebSocket server on a 127.0.0.1 ephemeral port that answers each upgrade request by hand:
/// `101`, `Upgrade: websocket`, the given `Connection` header value and the right
/// `Sec-WebSocket-Accept`, with an unmasked text frame `greeting` in the same write, then echoes
/// every text and binary message.
pub async fn upgrading(connection: &'static str, greeting: &'static str) -> SocketAddr {
    use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
    use tokio_tungstenite::tungstenite::protocol::Role;
    let (listener, addr) = listen().await;
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut seen = Vec::new();
                while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut byte = [0u8; 1];
                    stream.read_exact(&mut byte).await.unwrap();
                    seen.push(byte[0]);
                }
                let head = String::from_utf8(seen).unwrap();
                let key = head
                    .split("\r\n")
                    .find_map(|l| {
                        l.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                    })
                    .map(|(_, v)| v.trim().to_owned())
                    .unwrap();
                let mut out = format!(
                    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
                     Connection: {connection}\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                    derive_accept_key(key.as_bytes())
                )
                .into_bytes();
                out.extend_from_slice(&[0x81, greeting.len() as u8]);
                out.extend_from_slice(greeting.as_bytes());
                stream.write_all(&out).await.unwrap();
                let ws =
                    tokio_tungstenite::WebSocketStream::from_raw_socket(stream, Role::Server, None);
                let mut ws = ws.await;
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_text() || message.is_binary() {
                        ws.send(message).await.unwrap();
                    }
                }
            });
        }
    });
    addr
}
