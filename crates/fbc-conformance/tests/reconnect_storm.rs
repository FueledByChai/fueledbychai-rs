//! FBC-jcy's done line: fbc-runtime's market-data session, with the runtime's toy venue, played
//! through the stub's reconnect storm of 340 forced reconnects on paused time.

#[allow(dead_code)]
#[path = "../../fbc-runtime/tests/common/toy.rs"]
mod toy;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use fbc_conformance::{
    Frame, HttpRoutes, STORM_RECONNECTS, StubServer, check_pacing, reconnect_storm,
};
use fbc_core::{EndpointPlan, Envelope, MdEvent, MdTransport, VenueConfig, WireUrl};
use fbc_runtime::{
    Connector, IngestClock, Input, Liveness, MdSession, MdSessionConfig, ProxyConfig,
    ReconnectPacing,
};
use tokio::sync::oneshot;
use toy::ToyVenue;

const CONN: u16 = 3;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// Resolves once `limit` of wall-clock time has passed.
fn watchdog(limit: Duration) -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    std::thread::spawn(move || {
        std::thread::sleep(limit);
        let _ = tx.send(());
    });
    rx
}

#[tokio::test(start_paused = true)]
async fn a_session_rides_out_the_340_reconnect_storm_subscribed_once_per_epoch_and_paced() {
    // Each connection: the toy's hello and subscribe are read, then a trade whose seq names the
    // connection is pushed, then the stub closes it; the last stays open.
    let trade = |n: usize| Frame::text(format!("trade|sym=A|px=100|qty=1|seq={n}"));
    let script = reconnect_storm(STORM_RECONNECTS, 2, trade);
    let server = StubServer::start(script, HttpRoutes::new()).await.unwrap();

    // A drop waits the 1 s floor; at most 10 attempts start in any 60 s. No attempt deadline:
    // on paused time a pending deadline would let the clock jump ahead during socket I/O.
    let pacing = ReconnectPacing::new(secs(1), secs(8), 10, secs(60), Duration::MAX).unwrap();
    let seen: Rc<RefCell<Vec<Envelope<MdEvent>>>> = Rc::default();
    let keep = seen.clone();
    let venue = ToyVenue::leak();
    let config = MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(server.ws_url("/md")),
            },
            subs: [1, 2].map(toy::sub).into_iter().collect(),
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        // The toy asks for no HTTP here; any bound would do.
        http_max_body: 64 * 1024,
        conn: CONN,
        limiter: venue.limiter(0),
        // No silence window, for the same reason as no attempt deadline.
        liveness: Liveness::new(Duration::MAX, Duration::from_millis(1)).unwrap(),
    };
    let (mut session, control) =
        MdSession::new(config, move |env| keep.borrow_mut().push(env)).unwrap();

    let watch = seen.clone();
    let storm = async {
        server.finished().await.unwrap();
        while watch.borrow().len() <= STORM_RECONNECTS {
            tokio::task::yield_now().await;
        }
        // After the storm the runtime holds one live connection, while it still runs.
        let live = server.live();
        drop(control);
        live
    };
    // A regression that loses an event or a reconnect fails here instead of hanging (Codex
    // r4177514254). The bound is wall-clock, from another thread: a tokio timer would let the
    // paused clock jump ahead during socket I/O.
    let bounded = async {
        tokio::select! {
            live = storm => live,
            _ = watchdog(Duration::from_secs(60)) => panic!("the storm did not end within 60 s"),
        }
    };
    let (run, live) = tokio::join!(session.run(), bounded);
    run.unwrap();
    assert_eq!(live, 1);

    // Every connection received exactly one subscribe per desired subscription, from a fresh
    // codec built for the desired set, and nothing else.
    let conns = server.connections();
    assert_eq!(conns.len(), STORM_RECONNECTS + 1);
    for (n, conn) in conns.iter().enumerate() {
        let hello = Frame::text(format!("hello|codec={n}|plan=1,2"));
        assert_eq!(
            conn.received,
            [hello, Frame::text("sub|add=A,B")],
            "conn {n}"
        );
    }

    // No event of a superseded epoch reached the handler: connection n's one trade arrived
    // stamped with epoch n, in epoch order.
    let seen = seen.borrow();
    assert_eq!(seen.len(), STORM_RECONNECTS + 1);
    for (n, env) in seen.iter().enumerate() {
        assert_eq!(env.venue_seq, Some(n as u64));
        assert_eq!(
            (env.stamp.conn.conn, env.stamp.conn.epoch),
            (CONN, n as u32)
        );
    }
    assert!(
        seen.windows(2)
            .all(|w| w[0].stamp.ingest_seq < w[1].stamp.ingest_seq)
    );
    assert_eq!(session.stale(Input::Event), 0);
    assert_eq!(session.current().epoch, STORM_RECONNECTS as u32);

    // Attempts never exceeded the pacing: 1 s apart, ten per 60 s window.
    let starts: Vec<_> = conns.iter().map(|c| c.accepted_at).collect();
    check_pacing(&starts, &pacing).unwrap();
    let expected = (0..=STORM_RECONNECTS as u64).map(|n| secs(60 * (n / 10) + n % 10));
    let since: Vec<_> = starts.iter().map(|t| t.duration_since(starts[0])).collect();
    assert_eq!(since, expected.collect::<Vec<_>>());
    let counters = session.counters();
    let attempts = STORM_RECONNECTS as u64 + 1;
    assert_eq!((counters.attempts, counters.failed_attempts), (attempts, 0));
    assert_eq!(counters.decode_errors, 0);
}
