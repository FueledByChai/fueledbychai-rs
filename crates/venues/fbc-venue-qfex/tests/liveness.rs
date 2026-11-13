//! FBC-39ks's venue-crate side: an authenticated QFEX trade session keeps its connection alive
//! on the server's pings alone. QFEX's authenticate page
//! (https://docs.qfex.com/websocket/channels/trade/authenticate) says that once a connection has
//! authenticated, the server sends WebSocket pings from time to time; an order-entry codec's
//! keepalive is its own timer and cannot ask for a protocol ping (decision 0056), and the QFEX
//! codec arms none. So the session must answer each server ping with a pong, through the
//! WebSocket layer, and send nothing of its own while it is pinged.
//!
//! fbc-runtime's `ExecSession`, unchanged, runs the QFEX codec under `preprod_probe` (decision
//! 0132 item 6) against the conformance kit's stub server on 127.0.0.1, on a paused clock that
//! only the test moves: a blocking task that never ends keeps tokio from jumping it while socket
//! I/O is under way. The stub confirms each of the three subscriptions, so the codec reports the
//! stream authenticated (FBC-bsou) and the session sends its cancel-on-disconnect arm (decisions
//! 0058 and 0121), which the stub answers with the ack QFEX production sent (FBC-291a). Then the
//! stub pings it several times (`Step::Barrier`, which ends only on the pong to its own ping),
//! the test moving virtual time between two pings past the codec's 60 s authentication deadline.
//! The test shows:
//!
//! - each ping is answered with a pong, and the session stays in its one epoch: one connection
//!   attempt, no epoch ended until the test stops it;
//! - once its open has settled (authentication, three subscriptions and the arm), its codec
//!   writes no frame of its own while the stub pings it, the deadline's timer firing in between;
//! - its journal holds each ping on that epoch, at the virtual instant it arrived, by length
//!   only (decision 0041), and no frame written after the first.
//!
//! The probe's arm is answered by no frame the codec reads as its outcome: it ends `Unknown` at
//! `qfex.exec.rpc_timeout` and the session then ends the epoch (decision 0058; FBC-jxau decodes
//! the answer). The configuration sets that deadline far past the virtual time the test moves,
//! so the arm's own deadline never ends the epoch the pings are measured on. The probe's liveness
//! hold never arms (decision 0146): it is `qfex_preprod_probe`'s, tested in crates/fbc-examples.
//!
//! Every key here is synthetic, and every wait is for something the test can observe, bounded by
//! wall-clock time from another thread, so a regression fails instead of hanging.

use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fbc_conformance::{Frame, HttpRouter, Responder, Step, StubServer, WsScript};
use fbc_core::{
    AccountKey, ConnKey, ConnState, Envelope, ExecEvent, Namespace, Secret, Secrets, SpecTable,
    VenueConfig, VenueFactory,
};
use fbc_journal::{
    ControlEvent, JournalReader, JournalWriter, NonceSourceId, Record, RedactionKey, SinkConfig,
    WsControl, journal_queue,
};
use fbc_runtime::{
    AccountWideCodAuthority, Connector, ExecHandler, ExecSession, ExecSessionConfig, InFlight,
    IngestClock, Journal, ProxyConfig, RateLimiter, RateUserKey, ReconnectPacing, ReservedBound,
    RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::exec::{ARM, AUTH_DEADLINE, CHANNELS};
use fbc_venue_qfex::probe::{
    KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT, PREPROD_PROBE,
};
use fbc_venue_qfex::sign::QfexNonces;
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_TIMEOUT, KEY_REST_URL, Qfex, auth};
use serde_json::Value;
use tokio::time::advance;

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0001";

/// The authenticate page's success reply.
const AUTH_OK: &str = r#"{"type":"auth","result":"success"}"#;
/// The arm's echo in the AsyncAPI's generic `ack`, as QFEX production answered the arm
/// (`fixtures/qfex/exec-recorded/arm-production/README.md`): counted, settling nothing.
const ARM_ACK: &str = r#"{"ack":{"incoming_message":{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}}}"#;

/// The stub's pings, after the session's open has settled.
const PINGS: usize = 4;
/// The virtual time between two of them: three intervals pass the codec's 60 s authentication
/// deadline, and each is longer than the 20 s client ping one of QFEX's code samples sets.
const INTERVAL: Duration = Duration::from_secs(25);
/// The arm's deadline, far past the virtual time the test moves.
const RPC_TIMEOUT: &str = "3600s";
/// The session's connection number and the journal's shard.
const CONN: u16 = 7;
const SHARD: u16 = 0;
/// The consumer's account key, which the configuration binds the session to (decision 0122).
const ACCT: AccountKey = AccountKey::new(4);

/// The probe's configuration for the primary account on the stub's origins.
fn config(server: &StubServer) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_EXEC_MODE, PREPROD_PROBE);
    cfg.insert(auth::TRADE_URL, &server.ws_url(""));
    cfg.insert(KEY_REST_URL, &server.http_url(""));
    cfg.insert(KEY_MDS_URL, &server.ws_url(""));
    cfg.insert(KEY_REST_TIMEOUT, "5000ms");
    cfg.insert(KEY_PAGE_SIZE, "1000");
    cfg.insert(KEY_RESYNC_TIMEOUT, "30s");
    cfg.insert(KEY_RPC_TIMEOUT, RPC_TIMEOUT);
    cfg.insert(auth::ACCOUNT_ID, "primary");
    cfg.insert(auth::ACCOUNT_KEY, "4");
    cfg.insert(auth::PUBLIC_KEY, PUBLIC);
    cfg
}

fn secrets() -> Secrets {
    let mut creds = Secrets::new();
    creds.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    creds.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    creds
}

/// A responder sending `frames`, whatever it read.
fn reply(frames: &[&str]) -> Responder {
    let frames: Vec<Frame> = frames.iter().map(|f| Frame::text(*f)).collect();
    Responder::new(move |_| Ok(frames.clone()))
}

/// A responder confirming the subscription it read, as the AsyncAPI's `subscribed` reply does.
fn confirm() -> Responder {
    Responder::new(|frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary subscribe".to_owned());
        };
        let v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let channel = v["params"]["channels"][0].as_str().ok_or("no channel")?;
        Ok(vec![Frame::text(format!(
            r#"{{"subscribed":"{channel}"}}"#
        ))])
    })
}

/// Stops tokio's paused clock from jumping while socket I/O is under way: it moves only by
/// `advance`, until the returned sender drops.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// Lets every task run a while without moving the clock.
async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Lets every task run, without moving the clock, until `done`; panics after a minute of
/// wall-clock time, so a regression fails instead of hanging.
async fn until(what: &str, done: impl Fn() -> bool) {
    let expired = Arc::new(AtomicBool::new(false));
    let flag = expired.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(60));
        flag.store(true, Ordering::SeqCst);
    });
    while !done() {
        assert!(!expired.load(Ordering::SeqCst), "{what}: not within 60 s");
        tokio::task::yield_now().await;
    }
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[7; 32]).unwrap())
}

/// What the session's handler was given.
#[derive(Default)]
struct Heard {
    events: Vec<ExecEvent>,
    /// The epoch each `Authenticated` report was stamped on.
    authenticated: Vec<ConnKey>,
    ended: Vec<ConnKey>,
}

#[derive(Clone, Default)]
struct Watch(Rc<RefCell<Heard>>);

impl ExecHandler for Watch {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let mut heard = self.0.borrow_mut();
        if matches!(
            env.body,
            ExecEvent::Conn {
                state: ConnState::Authenticated,
                ..
            }
        ) {
            heard.authenticated.push(env.stamp.conn);
        }
        heard.events.push(env.body);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().ended.push(key);
    }
}

/// The script: the session's socket (connection 0) authenticated, its three subscriptions
/// confirmed and its arm acknowledged, then pinged [`PINGS`] times, each ping after the first
/// played only once the test has opened a connection of its own (a gate, connections 1 on): the
/// test moves virtual time before opening each. The steps played once each ping's pong came.
fn script() -> (Vec<Step>, Vec<usize>) {
    let mut steps = vec![
        Step::Accept,
        Step::Respond {
            conn: 0,
            with: reply(&[AUTH_OK]),
        },
    ];
    for _ in CHANNELS {
        steps.push(Step::Respond {
            conn: 0,
            with: confirm(),
        });
    }
    steps.push(Step::Respond {
        conn: 0,
        with: reply(&[ARM_ACK]),
    });
    let mut marks = Vec::new();
    for k in 0..PINGS {
        if k > 0 {
            steps.push(Step::Accept);
        }
        steps.push(Step::Barrier { conn: 0 });
        marks.push(steps.len());
    }
    (steps, marks)
}

#[tokio::test(start_paused = true)]
async fn an_authenticated_trade_session_answers_each_server_ping_and_writes_nothing_of_its_own() {
    assert!(INTERVAL * (PINGS as u32 - 1) > AUTH_DEADLINE);
    let frozen = freeze();
    let (steps, marks) = script();
    let server = StubServer::start(WsScript::new(steps), HttpRouter::new())
        .await
        .unwrap();
    let cfg = config(&server);
    let caps = Qfex.caps(&cfg).unwrap();
    let limits = caps
        .exec_limits
        .expect("the probe's caps declare its limits");
    let reserve = SafetyReserve::percent(10).unwrap();
    let limiter = RateLimiter::for_user(RateUserKey::new(0x39c5_0001), &limits, reserve).unwrap();
    let bound = ReservedBound::new(4, 4).unwrap();
    // One attempt a day and no attempt deadline: no second attempt, and no pending deadline
    // the test has to step past.
    let day = Duration::from_secs(86_400);
    let config = ExecSessionConfig {
        venue: &Qfex,
        cfg,
        creds: secrets(),
        acct: ACCT,
        account_wide_cod_authority: Some(AccountWideCodAuthority::new(ACCT)),
        rpc_ids: RpcIds::default(),
        ns: Namespace::new(7),
        specs: SpecTable::new(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(day, day, 1, day, Duration::MAX).unwrap(),
        clock: IngestClock::new(),
        nonces: Box::new(QfexNonces::os()),
        nonce_source: NonceSourceId(0),
        conn: CONN,
        limiter,
        write_stall: WriteStall::new(Duration::from_secs(3600)).unwrap(),
        http_max_body: 1 << 20,
        outbound: bound,
        in_flight: InFlight::new(bound),
    };
    let root = fresh_dir("qfex_liveness");
    let sink = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(sink, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let watch = Watch::default();
    let heard = Rc::clone(&watch.0);
    let (mut session, control) = ExecSession::new(config, watch).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(sink))));

    let server = &server;
    let sent = || server.connections()[0].received.len();
    let drive = async {
        let mut gates = Vec::new();
        for (k, mark) in marks.iter().enumerate() {
            if k > 0 {
                // Virtual time passes with nothing written: no keepalive of the codec's own.
                advance(INTERVAL).await;
                churn().await;
                assert_eq!(sent(), 5, "a frame of the codec's own after {k} intervals");
                assert!(heard.borrow().ended.is_empty(), "the epoch ended");
                let gate = Connector::new(ProxyConfig::Direct);
                gates.push(gate.websocket(&server.ws_url("/gate")).await.unwrap());
            }
            // The ping's pong came: the barrier played to its end.
            until(&format!("ping {k}"), || server.played() >= *mark).await;
            let heard = heard.borrow();
            assert_eq!(heard.authenticated.len(), 1);
            assert!(heard.ended.is_empty(), "the epoch ended at ping {k}");
            assert!(server.connections()[0].open);
            assert_eq!(sent(), 5, "a frame of the codec's own at ping {k}");
        }
        server.finished().await.unwrap();
        churn().await;
        drop(control);
        gates
    };
    let (run, _gates) = tokio::join!(session.run(), drive);
    run.unwrap();
    let counters = session.counters();
    drop(session);
    drop(frozen);
    writer.close().unwrap();

    // One attempt, one epoch, no arm failure and nothing it could not decode.
    assert_eq!((counters.attempts, counters.failed_attempts), (1, 0));
    assert_eq!(counters.arm_failures, 0);
    assert_eq!(counters.decode_errors, 0);
    assert_eq!(counters.refused_effects, 0);
    let heard = heard.borrow();
    let [epoch] = heard.authenticated[..] else {
        panic!("{:?}", heard.authenticated);
    };
    assert_eq!(epoch.conn, CONN);
    assert!(matches!(
        heard.events.as_slice(),
        [ExecEvent::Conn {
            state: ConnState::Authenticated,
            ..
        }]
    ));
    // The one epoch ended once, at the test's stop.
    assert_eq!(heard.ended, [epoch]);

    // What the session sent: its authentication, the three subscriptions and the arm, nothing
    // else, on one connection; the others are the test's gates, which sent nothing.
    let conns = server.connections();
    assert_eq!(conns.len(), PINGS);
    assert!(conns[1..].iter().all(|c| c.received.is_empty()));
    let text: Vec<&str> = (conns[0].received.iter())
        .map(|f| match f {
            Frame::Text(t) => t.as_str(),
            Frame::Binary(_) => "<binary>",
        })
        .collect();
    assert_eq!(text.len(), 5, "{text:?}");
    let auth: Value = serde_json::from_str(text[0]).unwrap();
    assert_eq!(auth["params"]["hmac"]["public_key"], PUBLIC);
    assert!(auth["params"].get("account_id").is_none());
    for (frame, channel) in text[1..4].iter().zip(CHANNELS) {
        let sub = format!(r#"{{"type":"subscribe","params":{{"channels":["{channel}"]}}}}"#);
        assert_eq!(*frame, sub);
    }
    assert_eq!(text[4], ARM);

    // The journal: each ping on the epoch, by length only, at least an interval after the one
    // before; the five frames written before the first; the authentication deadline's timer
    // firing between two pings with nothing written after it; no close received and no
    // request's deadline.
    let records: Vec<Record> = JournalReader::open(&root, SHARD)
        .unwrap()
        .entries()
        .map(|entry| entry.unwrap().record)
        .collect();
    let pings: Vec<(usize, &fbc_core::Stamp)> = (records.iter().enumerate())
        .filter_map(|(at, record)| match record {
            Record::InboundControl {
                stamp,
                frame: WsControl::Ping(payload),
            } => {
                assert_eq!(payload.0.len(), "barrier-1".len());
                Some((at, stamp))
            }
            _ => None,
        })
        .collect();
    assert_eq!(pings.len(), PINGS);
    assert!(pings.iter().all(|(_, stamp)| stamp.conn == epoch));
    for pair in pings.windows(2) {
        let gap = pair[1].1.recv_mono.0 - pair[0].1.recv_mono.0;
        assert!(gap >= INTERVAL.as_nanos() as u64, "{gap} ns between pings");
    }
    let first_ping = pings[0].0;
    let written: Vec<usize> = (records.iter().enumerate())
        .filter(|(_, record)| matches!(record, Record::Outbound { conn, .. } if *conn == epoch))
        .map(|(at, _)| at)
        .collect();
    assert_eq!(written.len(), 5);
    assert!(written.iter().all(|at| *at < first_ping));
    let timers: Vec<usize> = (records.iter().enumerate())
        .filter(|(_, record)| matches!(record, Record::Timer { stamp, .. } if stamp.conn == epoch))
        .map(|(at, _)| at)
        .collect();
    assert_eq!(timers.len(), 1, "the authentication deadline, once");
    assert!(timers[0] > pings[2].0 && timers[0] < pings[3].0);
    assert!(!records.iter().any(|record| matches!(
        record,
        Record::InboundControl {
            frame: WsControl::Close(_),
            ..
        } | Record::RpcTimeout { .. }
    )));
    let opened = |record: &Record| matches!(record, Record::Control { ev: ControlEvent::Opened(key), .. } if *key == epoch);
    let closed = |record: &Record| matches!(record, Record::Control { ev: ControlEvent::Closed(key), .. } if *key == epoch);
    assert_eq!(records.iter().filter(|r| opened(r)).count(), 1);
    assert_eq!(records.iter().filter(|r| closed(r)).count(), 1);
}
