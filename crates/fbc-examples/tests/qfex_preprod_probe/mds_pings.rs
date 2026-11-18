//! The QFEX model's pinging MDS socket (FBC-kllr; story BT-702): a relay on a loopback port of
//! its own in front of the model's MDS stub, through which the probe's MDS touch connects when
//! [`super::model::QfexModel::mds_pings`] is set. The conformance stub's serving scripts push data
//! frames and closes but send no ping of their own, and fbc-conformance stays as it is, so the
//! pings are this relay's.
//!
//! It hands the WebSocket upgrade through unchanged, byte for byte, so the stub answers it and
//! the client checks the stub's answer. After it, it reads both directions frame by frame
//! ([`parse`]) and hands every frame on whole, except a pong answering one of its own pings,
//! which it counts and keeps. While the test has it ping ([`Relay::pinging`]), every
//! [`Pings::every`] on the runtime's clock (paused, and moved by the test) it then does one of two
//! things on the connection:
//!
//! - when its oldest ping still unanswered was sent longer than [`Pings::within`] ago, it sends
//!   the client a close frame and ends the connection once the client ended its side, as a venue
//!   closes a connection whose pings go unanswered;
//! - otherwise it streams the model's touch, a `bbo` frame stamped as it is sent, then a ping of
//!   its own, so the client's pong to it says the client read that touch first.
//!
//! [`Relay::pinged`] says what it sent and heard on each connection, by its accept order. Nothing
//! here reaches a venue: it listens and connects on 127.0.0.1 alone.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// How the relay pings each connection while it pings.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Pings {
    /// How often, on the runtime's clock, from the upgrade on or from when pinging was turned on,
    /// whichever came later.
    pub every: Duration,
    /// How long a ping may go unanswered before the connection is closed.
    pub within: Duration,
}

/// What the relay sent and heard on one connection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pinged {
    /// Whether the connection pings now: the last [`Relay::pinging`] it took.
    pub on: bool,
    /// The touches it streamed, each before a ping.
    pub touches: u64,
    /// The pings it sent.
    pub sent: u64,
    /// The pings the client answered with a pong carrying the ping's payload.
    pub answered: u64,
    /// Whether it closed the connection for a ping unanswered past [`Pings::within`].
    pub closed_unanswered: bool,
    /// Whether the connection ended, either side's end.
    pub ended: bool,
}

/// The relay, listening.
pub struct Relay {
    addr: SocketAddr,
    pinged: Arc<Mutex<Vec<Pinged>>>,
    on: watch::Sender<bool>,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// A WebSocket frame's opcodes the relay reads or writes.
const TEXT: u8 = 0x1;
const CLOSE: u8 = 0x8;
const PING: u8 = 0x9;
const PONG: u8 = 0xA;

/// The status code of the close the relay sends: policy violation, as RFC 6455 §7.4.1 names it.
const UNANSWERED: u16 = 1008;

impl Relay {
    /// A relay in front of the WebSocket endpoint at `upstream`, pinging each connection as
    /// `pings` says once [`Relay::pinging`] turns it on, and streaming the touch `touch` gives the
    /// frame of, each time it is asked.
    pub async fn start(
        upstream: SocketAddr,
        pings: Pings,
        touch: impl Fn() -> String + Send + Sync + 'static,
    ) -> Relay {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let pinged = Arc::new(Mutex::new(Vec::new()));
        let shared = Arc::clone(&pinged);
        let touch = Arc::new(touch);
        let (on, on_rx) = watch::channel(false);
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let n = {
                    let mut all = shared.lock().unwrap();
                    all.push(Pinged::default());
                    all.len() - 1
                };
                let Ok(up) = TcpStream::connect(upstream).await else {
                    shared.lock().unwrap()[n].ended = true;
                    continue;
                };
                // No frame it hands on waits behind the one before it for an acknowledgement.
                let _ = (client.set_nodelay(true), up.set_nodelay(true));
                let (shared, touch) = (Arc::clone(&shared), Arc::clone(&touch));
                let on = on_rx.clone();
                tokio::spawn(async move {
                    let frame = move || touch();
                    relay(client, up, (pings, on), &frame, &shared, n).await;
                    shared.lock().unwrap()[n].ended = true;
                });
            }
        });
        Relay {
            addr,
            pinged,
            on,
            accept,
        }
    }

    /// Turns pinging on or off on every connection, open or to come: on, each pings
    /// [`Pings::every`] from when it took it; off, none pings or closes.
    pub fn pinging(&self, on: bool) {
        self.on.send_replace(on);
    }

    /// Its `ws://` origin.
    pub fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// What it sent and heard on each connection, by accept order.
    pub fn pinged(&self) -> Vec<Pinged> {
        self.pinged.lock().unwrap().clone()
    }
}

/// One frame read whole off the front of a buffer: its opcode, its payload unmasked, and how many
/// bytes it took.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parsed {
    pub opcode: u8,
    pub payload: Vec<u8>,
    pub len: usize,
}

/// The frame at the front of `buf`, once `buf` holds all of it (RFC 6455 §5.2): masked, as a
/// client sends, or not, as a server sends. `None` while it is not whole.
pub fn parse(buf: &[u8]) -> Option<Parsed> {
    let (first, second) = (*buf.first()?, *buf.get(1)?);
    let short = usize::from(second & 0x7f);
    let (len, mut at) = match short {
        126 => (
            usize::from(u16::from_be_bytes([*buf.get(2)?, *buf.get(3)?])),
            4,
        ),
        127 => {
            let bytes: [u8; 8] = buf.get(2..10)?.try_into().ok()?;
            (usize::try_from(u64::from_be_bytes(bytes)).ok()?, 10)
        }
        n => (n, 2),
    };
    let mask = match second & 0x80 != 0 {
        true => {
            let key: [u8; 4] = buf.get(at..at + 4)?.try_into().ok()?;
            at += 4;
            Some(key)
        }
        false => None,
    };
    let body = buf.get(at..at.checked_add(len)?)?;
    let payload = match mask {
        Some(key) => (body.iter().enumerate())
            .map(|(i, b)| b ^ key[i % 4])
            .collect(),
        None => body.to_vec(),
    };
    Some(Parsed {
        opcode: first & 0x0f,
        payload,
        len: at + len,
    })
}

/// A server's frame (unmasked, final) of `opcode` carrying `payload`.
pub fn server_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x80 | opcode];
    match payload.len() {
        n @ 0..=125 => frame.push(u8::try_from(n).unwrap()),
        n @ 126..=0xffff => {
            frame.push(126);
            frame.extend(u16::try_from(n).unwrap().to_be_bytes());
        }
        n => {
            frame.push(127);
            frame.extend(u64::try_from(n).unwrap().to_be_bytes());
        }
    }
    frame.extend(payload);
    frame
}

/// The end of an HTTP head in `buf`, past its blank line, once it arrived.
fn head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|at| at + 4)
}

/// Relays `client` to `up` and back, connection `n` of `pinged`, pinging as `pings` says while
/// the switch beside it holds true, until either side ends or the relay closes it for a ping
/// unanswered past `pings.within`.
async fn relay(
    client: TcpStream,
    up: TcpStream,
    (pings, mut on_rx): (Pings, watch::Receiver<bool>),
    touch: &(dyn Fn() -> String + Send + Sync),
    pinged: &Mutex<Vec<Pinged>>,
    n: usize,
) {
    let (mut client_rd, mut client_wr) = client.into_split();
    let (mut up_rd, mut up_wr) = up.into_split();
    let (mut from_client, mut from_up) = (Vec::new(), Vec::new());
    let (mut client_buf, mut up_buf) = ([0u8; 4096], [0u8; 4096]);
    // Once the stub's upgrade answer has gone through, every byte either way is a frame.
    let mut upgraded = false;
    // The pings sent and not answered yet: each one's payload and when it was sent.
    let mut unanswered: Vec<(Vec<u8>, tokio::time::Instant)> = Vec::new();
    let mut next = tokio::time::Instant::now();
    let mut count: u64 = 0;
    let mut on = *on_rx.borrow_and_update();
    pinged.lock().unwrap()[n].on = on;
    // Whether the relay is still there to turn pinging on or off.
    let mut watching = true;
    loop {
        tokio::select! {
            read = client_rd.read(&mut client_buf) => {
                let Ok(got @ 1..) = read else { return };
                from_client.extend_from_slice(&client_buf[..got]);
                if !upgraded {
                    // The upgrade request, handed on as it came.
                    if up_wr.write_all(&from_client).await.is_err() {
                        return;
                    }
                    from_client.clear();
                    continue;
                }
                while let Some(frame) = parse(&from_client) {
                    let whole: Vec<u8> = from_client.drain(..frame.len).collect();
                    let ours = (frame.opcode == PONG)
                        .then(|| unanswered.iter().position(|(p, _)| *p == frame.payload))
                        .flatten();
                    match ours {
                        Some(at) => {
                            unanswered.remove(at);
                            pinged.lock().unwrap()[n].answered += 1;
                        }
                        None if up_wr.write_all(&whole).await.is_err() => return,
                        None => {}
                    }
                }
            }
            read = up_rd.read(&mut up_buf) => {
                let Ok(got @ 1..) = read else { return };
                from_up.extend_from_slice(&up_buf[..got]);
                if !upgraded {
                    let Some(end) = head_end(&from_up) else { continue };
                    let head: Vec<u8> = from_up.drain(..end).collect();
                    if client_wr.write_all(&head).await.is_err() {
                        return;
                    }
                    upgraded = true;
                    next = tokio::time::Instant::now() + pings.every;
                }
                while let Some(frame) = parse(&from_up) {
                    let whole: Vec<u8> = from_up.drain(..frame.len).collect();
                    if client_wr.write_all(&whole).await.is_err() {
                        return;
                    }
                }
            }
            changed = on_rx.changed(), if watching => match changed {
                Ok(()) => {
                    on = *on_rx.borrow_and_update();
                    next = tokio::time::Instant::now() + pings.every;
                    pinged.lock().unwrap()[n].on = on;
                }
                // The relay was dropped: the connection pings, or not, as it last did.
                Err(_) => watching = false,
            },
            () = tokio::time::sleep_until(next), if upgraded && on => {
                let now = tokio::time::Instant::now();
                next = now + pings.every;
                let late = (unanswered.first()).is_some_and(|(_, at)| now - *at > pings.within);
                if late {
                    pinged.lock().unwrap()[n].closed_unanswered = true;
                    // The close, then the end of what the relay sends; what the client still
                    // sends (its pongs, its close reply) is read and dropped until it ends, so no
                    // reset meets its writes and discards what it has not read yet.
                    drop((up_rd, up_wr));
                    let close = server_frame(CLOSE, &UNANSWERED.to_be_bytes());
                    let _ = client_wr.write_all(&close).await;
                    let _ = client_wr.shutdown().await;
                    while let Ok(1..) = client_rd.read(&mut client_buf).await {}
                    return;
                }
                count += 1;
                let payload = format!("relay-ping-{count}").into_bytes();
                let touched = server_frame(TEXT, touch().as_bytes());
                let ping = server_frame(PING, &payload);
                if client_wr.write_all(&[touched, ping].concat()).await.is_err() {
                    return;
                }
                unanswered.push((payload, now));
                let mut all = pinged.lock().unwrap();
                all[n].touches += 1;
                all[n].sent += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fbc_runtime::ws::Message;
    use fbc_runtime::{Connector, ProxyConfig};
    use futures_util::{SinkExt, StreamExt};

    #[test]
    fn a_frame_is_read_whole_masked_or_not_at_each_length_form() {
        // A server's text frame of 5 bytes, unmasked, then the bytes after it left alone.
        let mut buf = server_frame(TEXT, b"hello");
        buf.extend(b"rest");
        let parsed = parse(&buf).unwrap();
        assert_eq!((parsed.opcode, parsed.len), (TEXT, 7));
        assert_eq!(parsed.payload, b"hello");
        // A client's pong of 3 bytes, masked.
        let key = [1u8, 2, 3, 4];
        let mut masked = vec![0x80 | PONG, 0x80 | 3];
        masked.extend(key);
        masked.extend(b"abc".iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
        let parsed = parse(&masked).unwrap();
        assert_eq!((parsed.opcode, parsed.len), (PONG, 9));
        assert_eq!(parsed.payload, b"abc");
        // The 16-bit and 64-bit length forms.
        let mid = server_frame(TEXT, &[7u8; 300]);
        assert_eq!(&mid[..4], &[0x81, 126, 1, 44]);
        assert_eq!(parse(&mid).unwrap().payload.len(), 300);
        let long = server_frame(TEXT, &[7u8; 70_000]);
        assert_eq!(long[1], 127);
        assert_eq!(parse(&long).unwrap().len, 70_010);
        // Not whole yet: at every cut short of the end, and in each header form.
        for cut in 0..buf.len() - 4 {
            assert_eq!(parse(&buf[..cut]), None, "{cut}");
        }
        assert_eq!(parse(&mid[..3]), None);
        assert_eq!(parse(&long[..9]), None);
        assert_eq!(parse(&masked[..5]), None);
    }

    #[test]
    fn an_http_head_ends_past_its_blank_line() {
        assert_eq!(head_end(b"HTTP/1.1 101\r\nA: b\r\n\r\nframes"), Some(22));
        assert_eq!(head_end(b"HTTP/1.1 101\r\nA: b\r\n"), None);
    }

    /// A stub that answers each text frame with the same text, behind a relay pinging every
    /// second and closing a connection whose ping goes unanswered for longer than three.
    async fn echoed() -> (fbc_conformance::StubServer, Relay) {
        let (script, _push) = fbc_conformance::WsScript::serving(|_| {
            fbc_conformance::Responder::new(|frame| Ok(vec![frame.clone()]))
        });
        let router = fbc_conformance::HttpRouter::new();
        let stub = fbc_conformance::StubServer::start(script, router)
            .await
            .unwrap();
        let upstream = stub.ws_url("").trim_start_matches("ws://").parse().unwrap();
        let pings = Pings {
            every: Duration::from_secs(1),
            within: Duration::from_secs(3),
        };
        let relay = Relay::start(upstream, pings, || r#"{"type":"bbo"}"#.to_owned()).await;
        (stub, relay)
    }

    /// Lets every task run, the clock unmoved, until `done`.
    async fn settled(done: impl Fn() -> bool) {
        for _ in 0..100_000 {
            if done() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("not settled");
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_that_reads_answers_every_ping_after_its_touch() {
        let frozen = super::super::freeze();
        let (stub, relay) = echoed().await;
        let mut ws = Connector::new(ProxyConfig::Direct)
            .websocket(&relay.url())
            .await
            .unwrap();
        // The upgrade went through to the stub, and data frames go both ways.
        ws.send(Message::text("hello")).await.unwrap();
        assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("hello"));
        assert_eq!(stub.connections().len(), 1);
        // Not pinging yet: the clock moves and nothing is sent.
        tokio::time::advance(Duration::from_secs(10)).await;
        settled(|| true).await;
        assert_eq!(relay.pinged()[0], Pinged::default());
        relay.pinging(true);
        settled(|| relay.pinged()[0].on).await;
        // Each second from then, the touch, then a ping, which the client's read answers.
        for k in 1..=5 {
            tokio::time::advance(Duration::from_secs(1)).await;
            settled(|| relay.pinged()[0].sent == k).await;
            let touch = ws.next().await.unwrap().unwrap();
            assert_eq!(touch, Message::text(r#"{"type":"bbo"}"#));
            assert!(matches!(ws.next().await, Some(Ok(Message::Ping(_)))));
            // The pong goes out with the client's next write.
            ws.flush().await.unwrap();
            settled(|| relay.pinged()[0].answered == k).await;
        }
        let pinged = &relay.pinged()[0];
        assert_eq!((pinged.touches, pinged.sent, pinged.answered), (5, 5, 5));
        assert!(!pinged.closed_unanswered && !pinged.ended);
        // The pongs to its own pings are the relay's: the stub heard only the data frame.
        assert_eq!(stub.connections()[0].received.len(), 1);
        // Turned off, it sends no ping and closes nothing, however long the clock moves.
        relay.pinging(false);
        settled(|| !relay.pinged()[0].on).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        settled(|| true).await;
        let pinged = &relay.pinged()[0];
        assert_eq!((pinged.sent, pinged.closed_unanswered), (5, false));
        // With the relay gone, the connection it accepted still relays both ways.
        drop(relay);
        ws.send(Message::text("again")).await.unwrap();
        assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("again"));
        drop(frozen);
    }

    #[tokio::test]
    async fn a_connection_whose_upstream_refuses_ends_at_once() {
        // An upstream port nothing listens on: the relay accepts the client, cannot reach the
        // stub, and ends the connection, so the client's upgrade fails.
        let gone = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let upstream = gone.local_addr().unwrap();
        drop(gone);
        let pings = Pings {
            every: Duration::from_secs(1),
            within: Duration::from_secs(3),
        };
        let relay = Relay::start(upstream, pings, String::new).await;
        let opened = Connector::new(ProxyConfig::Direct)
            .websocket(&relay.url())
            .await;
        assert!(opened.is_err());
        settled(|| relay.pinged().first().is_some_and(|p| p.ended)).await;
        assert_eq!(relay.pinged()[0].sent, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_ping_unanswered_past_its_window_closes_the_connection() {
        let frozen = super::super::freeze();
        let (_stub, relay) = echoed().await;
        // Pinging before the connection opens: it pings from its upgrade on.
        relay.pinging(true);
        let mut ws = Connector::new(ProxyConfig::Direct)
            .websocket(&relay.url())
            .await
            .unwrap();
        assert!(relay.pinged()[0].on);
        // The client reads nothing: the first ping, sent at 1 s, is still unanswered at 4 s,
        // three seconds later, and the connection is closed at 5 s, once it is past them.
        for k in 1..=4 {
            tokio::time::advance(Duration::from_secs(1)).await;
            settled(|| relay.pinged()[0].sent == k).await;
        }
        assert!(!relay.pinged()[0].closed_unanswered);
        tokio::time::advance(Duration::from_secs(1)).await;
        settled(|| relay.pinged()[0].closed_unanswered).await;
        // What the client then reads: the four touches and pings, then the relay's close, its
        // pongs and its close reply meanwhile read by the relay and dropped.
        let mut close = None;
        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Close(frame) = msg {
                close = frame.map(|f| u16::from(f.code));
            }
        }
        assert_eq!(close, Some(UNANSWERED));
        // The connection ends once the client ends its side.
        drop(ws);
        settled(|| relay.pinged()[0].ended).await;
        let pinged = &relay.pinged()[0];
        assert_eq!((pinged.sent, pinged.answered), (4, 0));
        drop(frozen);
    }
}
