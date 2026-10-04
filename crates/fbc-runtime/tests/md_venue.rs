//! FBC-klr's done line, across a venue: when the toy's plan moves a subscription to a second
//! endpoint, the runtime opens that endpoint and unsubscribes the first; an endpoint no longer
//! planned is closed; a plan the venue refuses opens nothing and is reported.

mod common;

use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;
use std::time::Duration;

use common::toy::{self, ToyVenue};
use common::{ScriptedWs, hanging};
use fbc_core::{
    ConnKey, Envelope, Feed, InstrumentId, MdEvent, StreamId, Subscription, VenueConfig, VenueError,
};
use fbc_runtime::{
    Connector, IngestClock, MdVenue, MdVenueConfig, PlanError, ProxyConfig, ReconnectPacing,
    SessionError, Step,
};

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn config(urls: &[String], conns: Range<u16>) -> MdVenueConfig {
    let mut cfg = VenueConfig::new();
    for (n, url) in urls.iter().enumerate() {
        cfg.insert(&format!("toy.url.{n}"), url);
    }
    let venue = ToyVenue::leak();
    MdVenueConfig {
        venue,
        cfg,
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), ms(5_000)).unwrap(),
        clock: IngestClock::new(),
        http_max_body: 64 * 1024,
        conns,
        limiter: venue.limiter(0),
    }
}

fn subs(insts: &[u32]) -> Vec<Subscription> {
    insts.iter().copied().map(toy::sub).collect()
}

async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

#[tokio::test]
async fn a_plan_that_moves_a_subscription_opens_the_second_endpoint_and_unsubscribes_the_first() {
    let (mut s0, mut s1) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let seen = Seen::default();
    let handler = {
        let seen = seen.clone();
        move |env| seen.borrow_mut().push(env)
    };
    let config = config(&[s0.url(), s1.url()], 10..20);
    let (mut venue, control) = MdVenue::new(config, handler).unwrap();
    control.set_desired(subs(&[2, 3])).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut a = s0.accept().await;
        assert_eq!(a.recv().await, "hello|codec=0|plan=2,3");
        assert_eq!(a.recv().await, "sub|add=B,C");
        // Two per endpoint: C moves to a second endpoint, which opens; the first drops it.
        control.set_desired(subs(&[1, 2, 3])).unwrap();
        assert_eq!(a.recv().await, "sub|add=A|remove=C");
        let mut b = s1.accept().await;
        assert_eq!(b.recv().await, "hello|codec=1|plan=3");
        assert_eq!(b.recv().await, "sub|add=C");
        b.send("trade|sym=C|px=3|qty=1|seq=1");
        until(|| watch.borrow().len() == 1).await;
        a.send("trade|sym=A|px=1|qty=1|seq=2");
        until(|| watch.borrow().len() == 2).await;

        // The second endpoint is no longer planned: closed; the first sends only the change.
        control.set_desired(subs(&[1])).unwrap();
        assert_eq!(a.recv().await, "sub|remove=B");
        assert_eq!(b.next().await, None);

        // A plan the venue refuses opens nothing and is reported; the last plan stands.
        let unknown = control.set_desired(subs(&[1, 9])).unwrap_err();
        let nine = InstrumentId::new(9);
        assert_eq!(
            unknown,
            PlanError::Venue(VenueError::UnknownInstrument(nine))
        );
        let funding = Subscription {
            inst: InstrumentId::new(1),
            feed: Feed::Funding,
        };
        let unsupported = control.set_desired([funding]).unwrap_err();
        assert_eq!(
            unsupported,
            PlanError::Venue(VenueError::UnsupportedFeed(funding))
        );
        assert_eq!(
            unsupported.to_string(),
            "the venue refused the plan: instrument 1 has no Funding feed on this venue"
        );
        a.send("trade|sym=A|px=1|qty=1|seq=3");
        until(|| watch.borrow().len() == 3).await;
        drop(control);
        assert_eq!(a.next().await, None);
    };
    let (run, ()) = tokio::join!(venue.run(), script);
    run.unwrap();
    let seen = seen.borrow();
    let at: Vec<_> = seen.iter().map(|e| (e.stamp.conn, e.venue_seq)).collect();
    let (first, second) = (
        ConnKey { conn: 10, epoch: 0 },
        ConnKey { conn: 11, epoch: 0 },
    );
    assert_eq!(at, [(second, Some(1)), (first, Some(2)), (first, Some(3))]);
}

#[tokio::test]
async fn a_plan_with_an_unopenable_url_or_a_repeated_stream_is_refused() {
    let (mut venue, control) =
        MdVenue::new(config(&["http://127.0.0.1:1/".into()], 1..2), |_| {}).unwrap();
    let err = control.set_desired(subs(&[1])).unwrap_err();
    assert!(
        matches!(&err, PlanError::Url { stream: StreamId(0), err } if err.step() == Step::Url),
        "{err:?}"
    );
    assert!(
        err.to_string()
            .starts_with("endpoint 0 cannot be opened: URL failed")
    );

    let mut cfg = config(&[], 1..2);
    cfg.cfg.insert("toy.url.0", "ws://127.0.0.1:1/");
    cfg.cfg.insert("toy.url.1", "ws://127.0.0.1:1/");
    cfg.cfg.insert(toy::ONE_STREAM, "yes");
    let (_, other) = MdVenue::new(cfg, |_| {}).unwrap();
    let err = other.set_desired(subs(&[1, 2, 3])).unwrap_err();
    assert_eq!(err, PlanError::DuplicateStream(StreamId(0)));
    assert_eq!(err.to_string(), "the plan names stream 0 twice");

    let missing = control.set_desired(subs(&[1, 2, 3])).unwrap_err();
    assert!(matches!(missing, PlanError::Venue(VenueError::Config(_))));
    drop(control);
    venue.run().await.unwrap();
}

#[tokio::test]
async fn a_venue_with_no_connection_number_left_for_an_endpoint_stops_with_an_error() {
    let (s0, s1) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let (mut venue, control) = MdVenue::new(config(&[s0.url(), s1.url()], 4..5), |_| {}).unwrap();
    control.set_desired(subs(&[1, 2, 3])).unwrap();
    let err = venue.run().await.unwrap_err();
    assert_eq!(err, SessionError::NoConnectionLeft);
    assert_eq!(
        err.to_string(),
        "the venue has no connection number left for an endpoint"
    );
    drop(control);
}

#[test]
fn a_venue_needs_a_configuration_it_accepts() {
    let mut cfg = config(&[], 1..2);
    cfg.cfg.insert(toy::REFUSE, "yes");
    let err = MdVenue::new(cfg, |_| {}).err().unwrap();
    assert!(matches!(err, SessionError::Config(_)));
}

#[tokio::test]
async fn a_plan_published_just_before_the_control_drops_opens_nothing() {
    let (addr, mut accepts) = hanging().await;
    let url = format!("ws://{addr}/md");
    // One connection number for two endpoints: applying this plan would stop the venue with
    // NoConnectionLeft, so a clean stop shows it was never applied (Codex r4177481301).
    let (mut venue, control) = MdVenue::new(config(&[url.clone(), url], 1..2), |_| {}).unwrap();
    control.set_desired(subs(&[1, 2, 3])).unwrap();
    drop(control);
    venue.run().await.unwrap();
    tokio::time::sleep(ms(50)).await;
    assert!(accepts.try_recv().is_err());
}
