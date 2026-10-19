//! The reference feed of the clustered-move benchmark: a WebSocket server on a thread and
//! runtime of its own, speaking Paradex's market data to one connection. It acknowledges the
//! codec's one subscription (BTC-USD-PERP's bbo), sends one bbo at once (the warm-up the
//! benchmark waits for before it starts), then a burst of bbo frames each time it is told to,
//! written together so they reach the session's socket together. Each bbo is the hand-built
//! fixture `bbo.sbe.txt` (an SBE `BboEvent`) with its sequence and both prices
//! moved: a clustered move down, one tick a frame.
//!
//! The server is told from another thread (the stub's answer to an order it read), so the
//! frames arrive while the shard is busy signing, which a server on the shard's own task could
//! never do.

use std::thread::{self, JoinHandle};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_tungstenite::tungstenite::Message;

use super::md::frame;

/// The fixture's bbo: seq 1001, bid 62000.5, ask 62001.0, as 10^-8 mantissas.
const SEQ_AT: usize = 8 + 8;
const BID_AT: usize = 8 + 16;
const ASK_AT: usize = 8 + 32;
/// One price tick (0.1) as a 10^-8 mantissa.
const TICK: i64 = 10_000_000;

/// The bbo frame `n` (from 0): the fixture's sequence plus `n`, both prices `n` ticks lower.
pub fn bbo(n: u64) -> Vec<u8> {
    let mut f = frame("bbo.sbe.txt");
    let field = |f: &[u8], at: usize| i64::from_le_bytes(f[at..at + 8].try_into().unwrap());
    let down = i64::try_from(n).unwrap() * TICK;
    let seq = field(&f, SEQ_AT) + i64::try_from(n).unwrap();
    let (bid, ask) = (field(&f, BID_AT) - down, field(&f, ASK_AT) - down);
    f[SEQ_AT..SEQ_AT + 8].copy_from_slice(&seq.to_le_bytes());
    f[BID_AT..BID_AT + 8].copy_from_slice(&bid.to_le_bytes());
    f[ASK_AT..ASK_AT + 8].copy_from_slice(&ask.to_le_bytes());
    f
}

/// The reference server, running until its client closes the connection.
pub struct Reference {
    url: String,
    bursts: UnboundedSender<usize>,
    thread: Option<JoinHandle<()>>,
}

impl Reference {
    /// Starts the server on the loopback.
    pub fn start() -> Reference {
        let (bursts, told) = unbounded_channel();
        let (bound, url) = std::sync::mpsc::channel();
        let thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(serve(bound, told));
        });
        let url = url.recv().unwrap();
        Reference {
            url,
            bursts,
            thread: Some(thread),
        }
    }

    /// `ws://127.0.0.1:<port>/v1`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// What tells the server to send a burst of that many frames, from any thread.
    pub fn bursts(&self) -> UnboundedSender<usize> {
        self.bursts.clone()
    }

    /// Waits until the server has seen its client close the connection.
    pub fn join(mut self) {
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

/// Accepts one connection, acknowledges its subscription, sends the warm-up, then a burst each
/// time it is told, until the client closes the connection.
async fn serve(bound: std::sync::mpsc::Sender<String>, mut told: UnboundedReceiver<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    bound.send(format!("ws://{addr}/v1")).unwrap();
    let (tcp, _) = listener.accept().await.unwrap();
    tcp.set_nodelay(true).unwrap();
    let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
    let Some(Ok(Message::Text(sub))) = ws.next().await else {
        panic!("the codec's subscription first");
    };
    let sub: serde_json::Value = serde_json::from_str(&sub).unwrap();
    assert_eq!(sub["method"], "subscribe", "{sub}");
    assert_eq!(sub["params"]["channel"], "bbo.BTC-USD-PERP", "{sub}");
    let ack = format!(r#"{{"jsonrpc":"2.0","result":{{}},"id":{}}}"#, sub["id"]);
    ws.send(Message::text(ack)).await.unwrap();
    ws.send(Message::binary(bbo(0))).await.unwrap();
    let mut next = 1;
    let mut told_open = true;
    loop {
        tokio::select! {
            burst = told.recv(), if told_open => match burst {
                Some(n) => {
                    for _ in 0..n {
                        ws.feed(Message::binary(bbo(next))).await.unwrap();
                        next += 1;
                    }
                    ws.flush().await.unwrap();
                }
                None => told_open = false,
            },
            read = ws.next() => match read {
                // The client's close (answered as the stream is read) or the connection's end.
                None | Some(Err(_) | Ok(Message::Close(_))) => break,
                Some(Ok(other)) => assert!(!other.is_text(), "a frame after the subscription: {other:?}"),
            },
        }
    }
}
