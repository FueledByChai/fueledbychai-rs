//! FBC-qfm's done line (BT-402, decisions 0002, 0031, 0100): one shard runs the toy market-data
//! venue and the conformance toy's order entry on the shard host. A cancel the market-data
//! handler submits while it handles a trade frame is written on the order-entry session, and on
//! Linux, as CI runs it, that session's handler is told its tick-to-wire: the order-entry
//! stream, the trade frame's stamp, and a non-negative time from the frame's kernel receive
//! until the cancel's write was done. A cancel the handler submits while it handles a trade read
//! from an HTTP answer, whose stamp has no `kernel_rx`, is written too and reported with none.
//! Elsewhere (developer Macs; not run in CI) no frame has a kernel receive time: both cancels go
//! out and none is reported.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use armed_oms::{Oms, Reserved, order_caps, session_config, settle, watermark};
use common::toy::{self, ToyVenue};
use common::{Peer, ScriptedHttp, ScriptedWs};
use fbc_core::{
    EndpointPlan, Envelope, ExecEvent, InstrumentId, MdEvent, MdTransport, RpcId, Stamp,
    SubmitHandle, VenueConfig, WallNs, WireUrl,
};
use fbc_oms::{Authorization, CancelChoice};
use fbc_runtime::{
    Connector, ExecHandler, ExecOrders, ExecSession, IngestClock, Ladder, Liveness, MdSession,
    MdSessionConfig, Member, ProxyConfig, ReconnectPacing, ShardHost, TickToWire, WriteStall,
};

/// The price of the trade read from a WebSocket frame: the handler cancels `V-1` on it.
const FRAME_PX: i64 = 101;
/// The price of the trade read from an HTTP answer: the handler cancels `V-2` on it.
const HTTP_PX: i64 = 103;

/// What the order-entry session told its handler.
#[derive(Default)]
struct ExecSeen {
    events: Vec<ExecEvent>,
    submitted: Vec<SubmitHandle>,
    ticks: Vec<TickToWire>,
}

/// The order-entry handler: keeps everything it is told.
struct ExecLog(Rc<RefCell<ExecSeen>>);

impl ExecHandler for ExecLog {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.0.borrow_mut().events.push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.0.borrow_mut().submitted.push(handle);
    }

    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        self.0.borrow_mut().ticks.push(sample);
    }
}

/// What the market-data handler did: each trade's stamp and price, and the cancel it submitted
/// on it.
#[derive(Default)]
struct MdSeen {
    trades: Vec<(Stamp, i64, RpcId)>,
}

/// A cancel of the order `cid` of `oms`, built and authorized.
fn cancel_of(oms: &mut Oms, cid: fbc_core::ClientOrderId) -> Authorization {
    let permit = oms.reg.cancellable(cid).unwrap();
    let CancelChoice::Send(cmd) = permit.cancel(&order_caps()) else {
        panic!("a cancel of an acknowledged order is sent");
    };
    oms.reg.authorize(armed_oms::ACCT, cmd).unwrap()
}

/// The market-data session of the toy at `url`, wanting trades on `A`, stamping from `clock`.
fn md_config(url: &str, clock: &IngestClock) -> MdSessionConfig {
    let venue = ToyVenue::leak();
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(url),
            },
            subs: vec![toy::sub(1)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap(),
        clock: clock.clone(),
        http_max_body: 1024,
        conn: 3,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

/// Answers the session's login, its cancel-on-disconnect arm (request 1) and its resync on
/// `peer`, then waits until the epoch takes places.
async fn armed_and_resynced(peer: &mut Peer, seen: &Rc<RefCell<ExecSeen>>) {
    assert!(peer.recv().await.starts_with("auth|ts="));
    peer.send("auth|ok=1|token=toy-session-token");
    assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
    let wm = watermark(&peer.recv().await);
    peer.send("item|rpc=1|i=0|res=ok");
    peer.send(&format!("rsbegin|wm={wm}"));
    peer.send("rsend");
    settle(|| seen.borrow().events.contains(&ExecEvent::ResyncEnd)).await;
}

/// The wall clock now.
fn now() -> WallNs {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    WallNs(i64::try_from(since.as_nanos()).unwrap())
}

/// What one shard run showed.
struct Shown {
    exec: Rc<RefCell<ExecSeen>>,
    md: Rc<RefCell<MdSeen>>,
    /// The frames the order-entry peer heard after the epoch took places.
    heard: Vec<String>,
    /// The wall clock before the shard started.
    start: WallNs,
}

/// Runs the shard: the toy's market data and the conformance toy's order entry on one host, a
/// decide pass that plans nothing. Once the order-entry epoch takes places the market-data
/// server sends a trade frame, then asks for an HTTP request whose answer holds a trade; the
/// handler submits a cancel on each.
async fn shard() -> Shown {
    // A blocked thread keeps the paused clock from auto-advancing while the sockets are idle.
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let clock = IngestClock::new();
    let mut host = ShardHost::new(clock.clone());

    let mut exec_server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let mut exec_config = session_config(&exec_server.url(), &reserved);
    exec_config.clock = clock.clone();
    let exec_seen: Rc<RefCell<ExecSeen>> = Rc::default();
    let (mut exec, exec_control) =
        ExecSession::new(exec_config, ExecLog(Rc::clone(&exec_seen))).unwrap();

    let (mut md_server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let md_seen: Rc<RefCell<MdSeen>> = Rc::default();
    let (marks, orders): (_, ExecOrders) = (Rc::clone(&md_seen), exec.orders());
    let mut oms = Oms::armed();
    let md_handler = move |env: Envelope<MdEvent>| {
        let MdEvent::Trade { px, .. } = env.body else {
            return;
        };
        let cid = match px.0 {
            FRAME_PX => oms.resting,
            HTTP_PX => oms.amendable,
            other => panic!("an unexpected trade at {other}"),
        };
        let rpc = orders.submit(cancel_of(&mut oms, cid)).unwrap();
        marks.borrow_mut().trades.push((env.stamp, px.0, rpc));
    };
    let (mut md, md_control) =
        MdSession::new(md_config(&md_server.url(), &clock), md_handler).unwrap();

    let watch = Rc::clone(&exec_seen);
    let script = async move {
        let mut exec_peer = exec_server.accept().await;
        armed_and_resynced(&mut exec_peer, &watch).await;
        let mut md_peer = md_server.accept().await;
        assert_eq!(md_peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(md_peer.recv().await, "sub|add=A");
        let mut heard = Vec::new();
        // A trade frame: the cancel the handler submits on it is written.
        md_peer.send(&format!("trade|sym=A|px={FRAME_PX}|qty=1|seq=1"));
        heard.push(exec_peer.recv().await);
        // A trade in an HTTP answer: so is the one it submits on that.
        md_peer.send(&format!("get|tag=1|ms=5000|url={}", http.url("/snap")));
        let exchange = http.request().await;
        assert_eq!(exchange.line, "GET /snap");
        let body = format!("trade|sym=A|px={HTTP_PX}|qty=1|seq=2");
        exchange.answer("HTTP/1.1 200 OK", &body).await;
        heard.push(exec_peer.recv().await);
        settle(|| watch.borrow().submitted.len() == 2).await;
        drop((md_control, exec_control));
        heard
    };
    let start = now();
    let mut decide = |_: &[InstrumentId], _: &mut Ladder| {};
    let members = vec![Member::md(&mut md), Member::exec(&mut exec)];
    let (ran, heard) = tokio::join!(host.run(members, &mut decide), script);
    ran.unwrap();
    drop(thaw);
    Shown {
        exec: exec_seen,
        md: md_seen,
        heard,
        start,
    }
}

/// Both cancels were sent and written, the frame's first, each under the request id the handler
/// was given; the trade from the frame and the one from the HTTP answer, in that order.
fn both_cancels_written(shown: &Shown) -> [(Stamp, RpcId); 2] {
    let md = shown.md.borrow();
    let prices: Vec<i64> = md.trades.iter().map(|(_, px, _)| *px).collect();
    assert_eq!(prices, [FRAME_PX, HTTP_PX]);
    let exec = shown.exec.borrow();
    let sent: Vec<RpcId> = exec
        .submitted
        .iter()
        .inspect(|h| assert!(h.receipt.is_ok(), "{h:?}"))
        .map(|h| h.rpc)
        .collect();
    let rpcs: Vec<RpcId> = md.trades.iter().map(|(_, _, rpc)| *rpc).collect();
    assert_eq!(sent, rpcs);
    for (frame, rpc) in shown.heard.iter().zip(&rpcs) {
        assert!(
            frame.starts_with(&format!("cancel|rpc={}|", rpc.0)),
            "{frame}"
        );
    }
    [(md.trades[0].0, rpcs[0]), (md.trades[1].0, rpcs[1])]
}

/// The tick-to-wire samples reported for writes attributed to a market-data input: the
/// order-entry session's own frames (its login, arm and resync) are attributed to its own
/// inputs, on its own connection.
fn md_ticks(shown: &Shown, md_conn: u16) -> Vec<TickToWire> {
    let exec = shown.exec.borrow();
    exec.ticks
        .iter()
        .filter(|t| t.frame.conn.conn == md_conn)
        .copied()
        .collect()
}

#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn a_cancel_the_handler_submits_on_a_market_data_frame_is_recorded_from_its_kernel_receive_and_none_where_kernel_rx_is_none()
 {
    let shown = shard().await;
    let [(frame, _), (answer, _)] = both_cancels_written(&shown);
    let rx = frame
        .kernel_rx
        .expect("a frame read on Linux has a kernel receive time");
    assert!(shown.start.0 <= rx.0 && rx.0 <= frame.recv_wall.0);
    // An HTTP answer's stamp has none.
    assert_eq!(answer.kernel_rx, None);
    // One sample for a market-data input: the frame's cancel, on the order-entry stream, from
    // the frame's kernel receive, non-negative, and no later than now. None for the answer's.
    let ticks = md_ticks(&shown, frame.conn.conn);
    assert_eq!(ticks.len(), 1, "{ticks:?}");
    let tick = ticks[0];
    assert_eq!(tick.stream, exec_toy::EXEC_STREAM);
    assert_eq!(tick.frame, frame);
    assert!(tick.nanos >= 0, "{tick:?}");
    assert!(rx.0 + tick.nanos <= now().0);
    // Every sample reported has a kernel receive time.
    let exec = shown.exec.borrow();
    assert!(exec.ticks.iter().all(|t| t.frame.kernel_rx.is_some()));
}

#[cfg(not(target_os = "linux"))]
#[tokio::test(start_paused = true)]
async fn off_linux_a_cancel_the_handler_submits_on_a_market_data_frame_is_written_and_no_tick_to_wire_is_reported()
 {
    let shown = shard().await;
    let [(frame, _), (answer, _)] = both_cancels_written(&shown);
    assert_eq!((frame.kernel_rx, answer.kernel_rx), (None, None));
    assert!(md_ticks(&shown, frame.conn.conn).is_empty());
    assert!(shown.exec.borrow().ticks.is_empty());
}
