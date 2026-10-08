//! Decision 0079, seen from tungstenite's side (Codex r4217150467 on PR #119): the logger this
//! binary installs through fbc-runtime's `log` receives tungstenite's own records, so the facade
//! tungstenite logs through is the one whose static maximum fbc-runtime caps, and none of those
//! records is at TRACE, where tungstenite would print each frame's payload. `scripts/check-deps.sh`
//! holds the other half: the resolved graph has one `log` package. Everything runs over an
//! in-memory pipe; nothing reaches the network.

use std::sync::Mutex;

use futures_util::{SinkExt, StreamExt};
use log::{Level, LevelFilter, Log, Metadata, Record};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

/// Stands in for a session token: it would be in a `writing frame` or `Sending frame` record if
/// tungstenite's TRACE call sites were compiled in.
const PAYLOAD: &str = "synthetic-bearer-0079";

static RECORDS: Mutex<Vec<(Level, String, String)>> = Mutex::new(Vec::new());

struct Capture;

impl Log for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        let line = (
            record.level(),
            record.target().to_owned(),
            record.args().to_string(),
        );
        RECORDS.lock().unwrap_or_else(|e| e.into_inner()).push(line);
    }

    fn flush(&self) {}
}

static CAPTURE: Capture = Capture;

#[tokio::test]
async fn tungstenite_logs_through_the_capped_facade_and_never_at_trace() {
    log::set_logger(&CAPTURE).expect("the only logger in this test binary");
    log::set_max_level(LevelFilter::Trace);

    let (client, server) = tokio::io::duplex(1 << 12);
    let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
    let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;

    client.send(Message::text(PAYLOAD)).await.unwrap();
    assert_eq!(
        server.next().await.unwrap().unwrap(),
        Message::text(PAYLOAD)
    );
    // A close from the server: tungstenite logs "Received close frame" and "Replying to close"
    // at DEBUG on the client, the records that show which facade it logs through.
    server
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        })))
        .await
        .unwrap();
    // The client stops at the close: past it, it would wait for the server to drop the pipe.
    match client.next().await {
        Some(Ok(Message::Close(_))) => {}
        other => panic!("the client read {other:?}, not the server's close"),
    }

    let records = RECORDS.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let shown = || format!("{records:#?}");
    assert!(
        records
            .iter()
            .any(|(level, target, _)| *level == Level::Debug && target.starts_with("tungstenite")),
        "the logger installed through fbc-runtime's log never heard from tungstenite, so \
         tungstenite logs through another facade: {}",
        shown()
    );
    assert!(
        records.iter().all(|(level, _, _)| *level != Level::Trace),
        "a record at TRACE reached the logger: {}",
        shown()
    );
    // tungstenite prints a frame's payload as text or as the hex of its bytes.
    let hex: String = PAYLOAD.bytes().map(|b| format!("{b:02x}")).collect();
    assert!(
        records
            .iter()
            .all(|(_, _, text)| !text.contains(PAYLOAD) && !text.contains(&hex)),
        "a frame's payload reached the logger: {}",
        shown()
    );
}
