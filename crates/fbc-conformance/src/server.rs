//! The stub venue server: a WebSocket endpoint that plays a [`WsScript`] and an HTTP/1.1
//! endpoint that answers fixed responses by path, each on its own 127.0.0.1 ephemeral port.
//! Nothing here reaches beyond the loopback interface.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::script::{Frame, Step, WsScript};

/// A fixed HTTP response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpReply {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Fixed responses by request path (the target without its query); any other path is answered
/// 404 with an empty body, and a request the stub cannot read 400.
pub type HttpRoutes = BTreeMap<String, HttpReply>;

/// The most bytes the stub reads of one request's head, and of its body.
const MAX_REQUEST: usize = 64 * 1024;

/// One connection the WebSocket endpoint accepted, numbered by its place in
/// [`StubServer::connections`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnRecord {
    /// When the TCP connection was accepted, on tokio's clock (paused in tests).
    pub accepted_at: Instant,
    /// Whether its WebSocket upgrade completed.
    pub upgraded: bool,
    /// Every data frame the client sent on it, in order.
    pub received: Vec<Frame>,
    /// False once either side closed it or its upgrade failed.
    pub open: bool,
}

/// Why a script stopped before its end. `step` numbers the failing step from 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScriptError {
    /// The step names a connection the script has not accepted.
    NoConnection { step: usize, conn: usize },
    /// The next connection did not complete a WebSocket upgrade.
    Accept { step: usize, conn: usize },
    /// The connection closed before the step could read from or write to it.
    Closed { step: usize, conn: usize },
}

impl fmt::Display for ScriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScriptError::NoConnection { step, conn } => {
                write!(f, "step {step} names connection {conn}, not yet accepted")
            }
            ScriptError::Accept { step, conn } => {
                write!(f, "step {step}: connection {conn} was not upgraded")
            }
            ScriptError::Closed { step, conn } => {
                write!(f, "step {step}: connection {conn} is closed")
            }
        }
    }
}

impl std::error::Error for ScriptError {}

type Shared<T> = Arc<Mutex<T>>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A poisoned lock still holds a usable record: nothing here panics while holding one.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A running stub. Dropping it stops every task it started and closes its connections.
pub struct StubServer {
    ws: SocketAddr,
    http: SocketAddr,
    conns: Shared<Vec<ConnRecord>>,
    /// One slot per HTTP connection in accept order, filled once its request is read.
    requests: Shared<Vec<Option<String>>>,
    result: watch::Receiver<Option<Result<(), ScriptError>>>,
    _stop: watch::Sender<()>,
}

impl StubServer {
    /// Binds both endpoints and starts playing `script`.
    pub async fn start(script: WsScript, routes: HttpRoutes) -> io::Result<StubServer> {
        let ws_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let http_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let (ws, http) = (ws_listener.local_addr()?, http_listener.local_addr()?);
        let (stop_tx, stop) = watch::channel(());
        let conns = Shared::default();
        let requests = Shared::default();
        let (accepted_tx, accepted) = mpsc::unbounded_channel();
        let (result_tx, result) = watch::channel(None);
        tokio::spawn(accept_ws(
            ws_listener,
            conns.clone(),
            accepted_tx,
            stop.clone(),
        ));
        tokio::spawn(accept_http(
            http_listener,
            Arc::new(routes),
            requests.clone(),
            stop.clone(),
        ));
        tokio::spawn(async move {
            let mut stop = stop;
            tokio::select! {
                done = play(script.steps, accepted) => {
                    result_tx.send_replace(Some(done));
                }
                _ = stop.changed() => {}
            }
        });
        Ok(StubServer {
            ws,
            http,
            conns,
            requests,
            result,
            _stop: stop_tx,
        })
    }

    /// `ws://127.0.0.1:<port><path>`; the endpoint answers any path.
    pub fn ws_url(&self, path: &str) -> String {
        format!("ws://{}{path}", self.ws)
    }

    /// `http://127.0.0.1:<port><path>`.
    pub fn http_url(&self, path: &str) -> String {
        format!("http://{}{path}", self.http)
    }

    /// Waits until the script has played to its end or failed.
    pub async fn finished(&self) -> Result<(), ScriptError> {
        let mut result = self.result.clone();
        // The player sends its result before its sender drops; only the stub's drop, which
        // `&self` prevents, ends it without one.
        let done = result.wait_for(Option::is_some).await;
        done.ok().and_then(|r| r.clone()).unwrap_or(Ok(()))
    }

    /// Every connection the WebSocket endpoint accepted, in accept order.
    pub fn connections(&self) -> Vec<ConnRecord> {
        lock(&self.conns).clone()
    }

    /// The upgraded connections still open.
    pub fn live(&self) -> usize {
        let conns = lock(&self.conns);
        conns.iter().filter(|c| c.upgraded && c.open).count()
    }

    /// Every HTTP request's method and target, e.g. `GET /markets?x=1`, in the order their
    /// connections arrived (one request per connection), however long each took to read.
    pub fn http_requests(&self) -> Vec<String> {
        lock(&self.requests).iter().flatten().cloned().collect()
    }
}

/// What the player asks a connection's task to do.
enum Cmd {
    Push(Frame, oneshot::Sender<bool>),
    Close(oneshot::Sender<bool>),
    Silent,
}

/// The player's handle on an upgraded connection.
struct Conn {
    frames: mpsc::UnboundedReceiver<Frame>,
    cmds: mpsc::UnboundedSender<Cmd>,
}

type Ready = oneshot::Receiver<Option<Conn>>;

/// Accepts every TCP connection, records it at once and upgrades it in a task of its own; hands
/// the player each upgrade's outcome in accept order.
async fn accept_ws(
    listener: TcpListener,
    conns: Shared<Vec<ConnRecord>>,
    accepted: mpsc::UnboundedSender<Ready>,
    mut stop: watch::Receiver<()>,
) {
    loop {
        let stream = tokio::select! {
            r = listener.accept() => r,
            _ = stop.changed() => return,
        };
        // An accept error drops the listener: the player's next Accept fails.
        let Ok((stream, _)) = stream else { return };
        let index = {
            let mut conns = lock(&conns);
            conns.push(ConnRecord {
                accepted_at: Instant::now(),
                upgraded: false,
                received: Vec::new(),
                open: true,
            });
            conns.len() - 1
        };
        let (ready_tx, ready) = oneshot::channel();
        let _ = accepted.send(ready);
        tokio::spawn(serve_ws(
            stream,
            index,
            conns.clone(),
            ready_tx,
            stop.clone(),
        ));
    }
}

async fn serve_ws(
    stream: TcpStream,
    index: usize,
    conns: Shared<Vec<ConnRecord>>,
    ready: oneshot::Sender<Option<Conn>>,
    mut stop: watch::Receiver<()>,
) {
    let record = |f: &dyn Fn(&mut ConnRecord)| f(&mut lock(&conns)[index]);
    let upgraded = tokio::select! {
        ws = tokio_tungstenite::accept_async(stream) => ws.ok(),
        _ = stop.changed() => None,
    };
    let Some(mut ws) = upgraded else {
        record(&|c| c.open = false);
        let _ = ready.send(None);
        return;
    };
    record(&|c| c.upgraded = true);
    let (heard, frames) = mpsc::unbounded_channel();
    let (cmds, mut cmd_rx) = mpsc::unbounded_channel();
    let _ = ready.send(Some(Conn { frames, cmds }));
    let mut player = true;
    loop {
        tokio::select! {
            msg = ws.next() => {
                let frame = match msg {
                    Some(Ok(Message::Text(text))) => Frame::Text(text.as_str().to_owned()),
                    Some(Ok(Message::Binary(bytes))) => Frame::Binary(bytes.to_vec()),
                    Some(Ok(_)) => continue,
                    _ => break,
                };
                record(&|c| c.received.push(frame.clone()));
                let _ = heard.send(frame);
            }
            cmd = cmd_rx.recv(), if player => match cmd {
                Some(Cmd::Push(frame, sent)) => {
                    let message = match frame {
                        Frame::Text(text) => Message::text(text),
                        Frame::Binary(bytes) => Message::binary(bytes),
                    };
                    let _ = sent.send(ws.send(message).await.is_ok());
                }
                Some(Cmd::Close(sent)) => {
                    let _ = sent.send(ws.close(None).await.is_ok());
                }
                Some(Cmd::Silent) => {
                    let _ = stop.changed().await;
                    break;
                }
                // The script ended: the connection goes on recording until it closes.
                None => player = false,
            },
            _ = stop.changed() => break,
        }
    }
    record(&|c| c.open = false);
}

/// Plays `steps` in order against the connections `accepted` yields.
async fn play(
    steps: Vec<Step>,
    mut accepted: mpsc::UnboundedReceiver<Ready>,
) -> Result<(), ScriptError> {
    let mut conns: Vec<Conn> = Vec::new();
    for (step, action) in steps.into_iter().enumerate() {
        match action {
            Step::Accept => {
                let opened = next_conn(&mut accepted).await;
                let conn = conns.len();
                conns.push(opened.ok_or(ScriptError::Accept { step, conn })?);
            }
            Step::Read { conn } => {
                let (handle, closed) = handle(&mut conns, step, conn)?;
                handle.frames.recv().await.ok_or(closed)?;
            }
            Step::Push { conn, frame } => {
                let (handle, closed) = handle(&mut conns, step, conn)?;
                if !done(handle, |sent| Cmd::Push(frame, sent)).await {
                    return Err(closed);
                }
            }
            // A close the socket did not take was not forced by the stub (Codex r4177464414).
            Step::Close { conn } => {
                let (handle, closed) = handle(&mut conns, step, conn)?;
                if !done(handle, Cmd::Close).await {
                    return Err(closed);
                }
            }
            Step::Silent { conn } => {
                let (handle, closed) = handle(&mut conns, step, conn)?;
                handle.cmds.send(Cmd::Silent).map_err(|_| closed)?;
            }
        }
    }
    Ok(())
}

/// Asks the connection's task for `cmd` and waits for its outcome: false when the write failed
/// or the connection had already ended.
async fn done(handle: &Conn, cmd: impl FnOnce(oneshot::Sender<bool>) -> Cmd) -> bool {
    let (sent, ok) = oneshot::channel();
    handle.cmds.send(cmd(sent)).is_ok() && ok.await.unwrap_or(false)
}

/// The next accepted connection once upgraded; `None` when its upgrade failed or the listener
/// stopped.
async fn next_conn(accepted: &mut mpsc::UnboundedReceiver<Ready>) -> Option<Conn> {
    accepted.recv().await?.await.ok().flatten()
}

/// The player's handle on connection `conn`, and the error step `step` fails with if it closed.
fn handle(
    conns: &mut [Conn],
    step: usize,
    conn: usize,
) -> Result<(&mut Conn, ScriptError), ScriptError> {
    let found = conns.get_mut(conn);
    let handle = found.ok_or(ScriptError::NoConnection { step, conn })?;
    Ok((handle, ScriptError::Closed { step, conn }))
}

async fn accept_http(
    listener: TcpListener,
    routes: Arc<HttpRoutes>,
    requests: Shared<Vec<Option<String>>>,
    mut stop: watch::Receiver<()>,
) {
    loop {
        let stream = tokio::select! {
            r = listener.accept() => r,
            _ = stop.changed() => return,
        };
        let Ok((stream, _)) = stream else { return };
        // The connection's slot is taken now, so a slow request keeps its place (Codex
        // r4177464418).
        let slot = {
            let mut requests = lock(&requests);
            requests.push(None);
            requests.len() - 1
        };
        let (routes, requests, mut stop) = (routes.clone(), requests.clone(), stop.clone());
        tokio::spawn(async move {
            tokio::select! {
                _ = serve_http(stream, &routes, &requests, slot) => {}
                _ = stop.changed() => {}
            }
        });
    }
}

/// Answers one request on `stream`, then closes it.
async fn serve_http(
    mut stream: TcpStream,
    routes: &HttpRoutes,
    requests: &Mutex<Vec<Option<String>>>,
    slot: usize,
) {
    let reply = match read_request(&mut stream).await {
        Some(line) => {
            let path = line.split(' ').nth(1).unwrap_or_default();
            let path = path.split('?').next().unwrap_or_default();
            let reply = routes.get(path).cloned().unwrap_or(HttpReply {
                status: 404,
                body: Vec::new(),
            });
            lock(requests)[slot] = Some(line);
            reply
        }
        None => HttpReply {
            status: 400,
            body: Vec::new(),
        },
    };
    let head = format!(
        "HTTP/1.1 {} \r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reply.status,
        reply.body.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&reply.body).await;
    let _ = stream.shutdown().await;
    // A lingering close: what the client sent past what was read is drained, so closing does
    // not reset the connection under a response it has not read yet.
    let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
}

/// Reads a request's head and body; its method and target, or `None` when it is not a request
/// the stub can read within [`MAX_REQUEST`].
async fn read_request(stream: &mut TcpStream) -> Option<String> {
    let mut seen = Vec::new();
    let head_end = loop {
        if let Some(i) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        if seen.len() > MAX_REQUEST {
            return None;
        }
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
        seen.extend_from_slice(&chunk[..n]);
    };
    // The delimiter can arrive in the read that passes the limit (Codex r4177464412).
    if head_end > MAX_REQUEST {
        return None;
    }
    let head = std::str::from_utf8(&seen[..head_end]).ok()?;
    let mut lines = head.split("\r\n");
    let (method, rest) = lines.next()?.split_once(' ')?;
    let (target, _version) = rest.rsplit_once(' ')?;
    let length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map_or(Some(0), |(_, v)| v.trim().parse::<usize>().ok())?;
    if length > MAX_REQUEST {
        return None;
    }
    let mut body = seen.len() - head_end;
    while body < length {
        let mut chunk = [0u8; 1024];
        body += stream.read(&mut chunk).await.ok().filter(|n| *n > 0)?;
    }
    Some(format!("{method} {target}"))
}
