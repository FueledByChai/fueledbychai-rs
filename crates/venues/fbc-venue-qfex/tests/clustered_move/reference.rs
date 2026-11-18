//! The reference feed of the clustered-move benchmark: a WebSocket server on a thread and
//! runtime of its own, speaking QFEX's public MDS to one connection. It reads the codec's one
//! subscription (`AAPL-USD`'s `bbo`; QFEX's MDS confirms none), sends one bbo at once (the
//! warm-up the benchmark waits for before it starts), then a burst of bbo frames each time it is
//! told to, written together so they reach the session's socket together. Each bbo is the
//! documented frame `fixtures/qfex/md/bbo.json` shows, on `AAPL-USD`'s 0.01 grid, its sequence
//! one more than the last and both prices a tick lower: a clustered move down, one tick a frame.
//!
//! The server is told from another thread (the stub's answer to a place it read), so the frames
//! arrive while the shard is busy placing, which a server on the shard's own task could never
//! do.

use std::thread::{self, JoinHandle};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_tungstenite::tungstenite::Message;

/// The warm-up's bid and ask, in ticks of 0.01: 200.00 and 200.01.
const BID: u64 = 20_000;
const ASK: u64 = 20_001;

/// `ticks` of 0.01 as a decimal string.
fn px(ticks: u64) -> String {
    format!("{}.{:02}", ticks / 100, ticks % 100)
}

/// The bbo frame `n` (from 0): sequence 30 272 539 plus `n`, both prices `n` ticks below the
/// warm-up's.
pub fn bbo(n: u64) -> String {
    format!(
        r#"{{"type":"bbo","symbol":"AAPL-USD","bid":[["{}","5.00"]],"ask":[["{}","5.00"]],"sequence":{},"time":"2025-09-04T09:26:39.545268322Z"}}"#,
        px(BID - n),
        px(ASK - n),
        30_272_539 + n,
    )
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

    /// `ws://127.0.0.1:<port>`, a loopback origin the probe's guard admits.
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

/// Accepts one connection, reads its subscription, sends the warm-up, then a burst each time it
/// is told, until the client closes the connection.
async fn serve(bound: std::sync::mpsc::Sender<String>, mut told: UnboundedReceiver<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    bound.send(format!("ws://{addr}")).unwrap();
    let (tcp, _) = listener.accept().await.unwrap();
    tcp.set_nodelay(true).unwrap();
    let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
    let Some(Ok(Message::Text(sub))) = ws.next().await else {
        panic!("the codec's subscription first");
    };
    let sub: serde_json::Value = serde_json::from_str(&sub).unwrap();
    assert_eq!(sub["type"], "subscribe", "{sub}");
    assert_eq!(sub["channels"], serde_json::json!(["bbo"]), "{sub}");
    assert_eq!(sub["symbols"], serde_json::json!(["AAPL-USD"]), "{sub}");
    ws.send(Message::text(bbo(0))).await.unwrap();
    let mut next = 1;
    let mut told_open = true;
    loop {
        tokio::select! {
            burst = told.recv(), if told_open => match burst {
                Some(n) => {
                    for _ in 0..n {
                        ws.feed(Message::text(bbo(next))).await.unwrap();
                        next += 1;
                    }
                    ws.flush().await.unwrap();
                }
                None => told_open = false,
            },
            read = ws.next() => match read {
                // The client's close (answered as the stream is read) or the connection's end.
                None | Some(Err(_) | Ok(Message::Close(_))) => break,
                Some(Ok(other)) => {
                    assert!(!other.is_text(), "a frame after the subscription: {other:?}");
                }
            },
        }
    }
}
