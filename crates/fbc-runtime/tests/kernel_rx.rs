//! FBC-2y3's done line: on Linux, frames from local `ws://` and `wss://` servers carry the kernel
//! receive time of their last packet, no later than their `recv_wall`, and a Safety-class write
//! issued while a frame is handled, by the handler or by the codec's effects, is reported with a
//! non-negative tick-to-wire from that frame's `kernel_rx`; a Normal write is not. Elsewhere
//! (developer Macs; not run in CI) every stamp's `kernel_rx` is `None` and nothing is reported,
//! though the writes still go.

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::ScriptedWs;
use common::tls::TestCa;
use common::toy::{self, ToyVenue};
use fbc_core::{
    EndpointPlan, Envelope, MdEvent, MdTransport, OpKind, RateCharge, TrafficClass, VenueConfig,
    WallNs, WireSlice, WireUrl,
};
use fbc_runtime::{
    Connector, IngestClock, Liveness, MdHandler, MdSession, MdSessionConfig, Outbox, ProxyConfig,
    ReconnectPacing, TickToWire, WriteStall,
};

/// What the handler saw: every envelope, and every tick-to-wire reported.
#[derive(Clone, Default)]
struct Seen {
    envs: Rc<RefCell<Vec<Envelope<MdEvent>>>>,
    ticks: Rc<RefCell<Vec<TickToWire>>>,
}

/// A handler that answers each trade with a Safety-class write, as an engine pulls its quotes.
struct Pulling(Seen);

impl MdHandler for Pulling {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.0.envs.borrow_mut().push(env);
    }

    fn on_md_with(&mut self, env: Envelope<MdEvent>, out: &mut Outbox) {
        if let (MdEvent::Trade { .. }, Some(seq)) = (&env.body, env.venue_seq) {
            let frame = WireSlice::plain(format!("pull|seq={seq}").into_bytes());
            let charge = RateCharge::one(OpKind::Cancel, None);
            out.send(frame, TrafficClass::Safety, charge);
        }
        self.on_md(env);
    }

    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        self.0.ticks.borrow_mut().push(sample);
    }
}

fn now() -> WallNs {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    WallNs(i64::try_from(since.as_nanos()).unwrap())
}

/// Runs one session of the toy venue against a scripted server, plain or behind TLS: a trade
/// the handler answers with a Safety write, a `cancel` the codec answers with one, and a `say`
/// it answers with a Normal one. What the handler saw, and the wall clock before it started.
async fn run(tls: bool) -> (Seen, WallNs) {
    let mut connector = Connector::new(ProxyConfig::Direct);
    let (mut server, url) = if tls {
        let ca = TestCa::new();
        connector.add_trust_anchor(&ca.der()).unwrap();
        let server = ScriptedWs::start_tls(ca.server(&["localhost"])).await;
        let url = server.wss_url("localhost");
        (server, url)
    } else {
        let server = ScriptedWs::start().await;
        let url = server.url();
        (server, url)
    };
    let venue = ToyVenue::leak();
    let config = MdSessionConfig {
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
        connector,
        pacing: ReconnectPacing::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: 3,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    let seen = Seen::default();
    let start = now();
    let (mut session, control) = MdSession::new(config, Pulling(seen.clone())).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send("trade|sym=A|px=100|qty=1|seq=1");
        assert_eq!(peer.recv().await, "pull|seq=1");
        peer.send("cancel|id=2");
        assert_eq!(peer.recv().await, "cancel|id=2");
        peer.send("say");
        assert_eq!(peer.recv().await, "said");
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let (ran, ()) = tokio::join!(session.run(), script);
    ran.unwrap();
    (seen, start)
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn frames_carry_their_kernel_receive_time_and_safety_writes_their_tick_to_wire() {
    for tls in [false, true] {
        let (seen, start) = run(tls).await;
        let envs = seen.envs.borrow();
        assert_eq!(envs.len(), 1, "tls: {tls}");
        let trade = envs[0].stamp;
        let rx = trade
            .kernel_rx
            .expect("a frame read on Linux has a kernel receive time");
        // Received after the test began, and before the session finished reading the frame.
        assert!(start.0 <= rx.0 && rx.0 <= trade.recv_wall.0, "tls: {tls}");

        // The handler's pull and the codec's cancel, each from the frame it was issued for;
        // the Normal `said` is not reported.
        let ticks = seen.ticks.borrow();
        assert_eq!(ticks.len(), 2, "tls: {tls}");
        assert!(
            ticks
                .iter()
                .all(|t| t.stream == toy::STREAM && t.nanos >= 0)
        );
        assert_eq!(ticks[0].frame, trade);
        let cancel = ticks[1].frame;
        assert!(cancel.ingest_seq > trade.ingest_seq);
        let cancel_rx = cancel.kernel_rx.unwrap();
        assert!(rx.0 <= cancel_rx.0 && cancel_rx.0 <= cancel.recv_wall.0);
    }
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn off_linux_no_frame_has_a_kernel_receive_time_and_no_tick_to_wire_is_reported() {
    for tls in [false, true] {
        // The script still saw the handler's pull and the codec's cancel go out.
        let (seen, _) = run(tls).await;
        let envs = seen.envs.borrow();
        assert_eq!(envs.len(), 1, "tls: {tls}");
        assert_eq!(envs[0].stamp.kernel_rx, None);
        assert!(seen.ticks.borrow().is_empty(), "tls: {tls}");
    }
}
