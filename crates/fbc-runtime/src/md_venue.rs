//! A venue's market data: the endpoints [`VenueFactory::plan_md`] spreads the desired
//! subscriptions over, each driven by its own [`MdSession`] (decisions 0014, 0027).
//!
//! [`MdVenueControl::set_desired`] plans the whole set at once. A plan the venue refuses (an
//! instrument missing from the spec table, a feed it does not offer, its configuration), or one
//! the runtime could not run (a stream named twice, a socket URL no attempt could open), is
//! returned to the caller and opens nothing; the last accepted plan stands. [`MdVenue::run`]
//! applies each accepted plan by [`StreamId`]: an endpoint planned again with the same transport
//! keeps its connection and gets the difference through its reconciler, a new one (or one whose
//! transport changed) opens under the next connection number of the venue's range, and one no
//! longer planned is closed. Every session runs in the one task that runs the venue, and all of
//! them hand their events to the consumer's one handler and record into its one [`Journal`],
//! when it set one ([`MdVenue::set_journal`]).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::rc::Rc;

use fbc_core::{
    ConnKey, EndpointPlan, Envelope, MdEvent, MdTransport, SpecTable, StreamId, Subscription,
    VenueConfig, VenueError, VenueFactory,
};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::watch;

use crate::connector::Connector;
use crate::error::NetError;
use crate::journal::Journal;
use crate::liveness::Liveness;
use crate::pacing::ReconnectPacing;
use crate::ratelimit::RateLimiter;
use crate::session::{MdControl, MdHandler, MdSession, MdSessionConfig, Outbox, SessionError};
use crate::session_core::{IngestClock, TickToWire};
use crate::stall::WriteStall;
use crate::ws;

/// What a venue's market data needs, all from the consumer.
pub struct MdVenueConfig {
    pub venue: &'static dyn VenueFactory,
    pub cfg: VenueConfig,
    pub specs: SpecTable,
    pub connector: Connector,
    /// Each socket endpoint's reconnect pacing.
    pub pacing: ReconnectPacing,
    pub clock: IngestClock,
    /// Each endpoint's limit on an HTTP response body.
    pub http_max_body: usize,
    /// The connection numbers this venue's endpoints take, in order, one per endpoint opened;
    /// disjoint from every other connection's on the shard.
    pub conns: Range<u16>,
    /// The buckets of the venue's declared limits, which every endpoint charges (decision
    /// 0030); built for exactly the venue's limits.
    pub limiter: RateLimiter,
    /// Each socket endpoint's silence window and rotation margin (0033).
    pub liveness: Liveness,
    /// Each socket endpoint's bound on a write its peer stopped reading (0036).
    pub write_stall: WriteStall,
}

/// Why a desired set was not planned; nothing was opened or closed for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// The venue refused to plan it.
    Venue(VenueError),
    /// The plan names a stream twice.
    DuplicateStream(StreamId),
    /// No attempt could open this socket endpoint's URL; the error names the step, never the
    /// URL.
    Url { stream: StreamId, err: NetError },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::Venue(e) => write!(f, "the venue refused the plan: {e}"),
            PlanError::DuplicateStream(s) => write!(f, "the plan names stream {} twice", s.0),
            PlanError::Url { stream, err } => {
                write!(f, "endpoint {} cannot be opened: {err}", stream.0)
            }
        }
    }
}

impl std::error::Error for PlanError {}

/// Changes a running venue's desired subscriptions; dropping it stops every endpoint.
pub struct MdVenueControl {
    venue: &'static dyn VenueFactory,
    cfg: VenueConfig,
    specs: SpecTable,
    plan: watch::Sender<Vec<EndpointPlan>>,
}

impl MdVenueControl {
    /// Plans `subs` with the venue and hands the plan to the running venue, which applies only
    /// the latest; a refused plan changes nothing and is returned.
    pub fn set_desired(
        &self,
        subs: impl IntoIterator<Item = Subscription>,
    ) -> Result<(), PlanError> {
        let subs: BTreeSet<_> = subs.into_iter().collect();
        let plan = self
            .venue
            .plan_md(&self.cfg, &self.specs, &subs)
            .map_err(PlanError::Venue)?;
        let mut streams = BTreeSet::new();
        for ep in &plan {
            if !streams.insert(ep.stream) {
                return Err(PlanError::DuplicateStream(ep.stream));
            }
            if let MdTransport::Socket { url } = &ep.transport {
                ws::check_url(url.as_str()).map_err(|err| PlanError::Url {
                    stream: ep.stream,
                    err,
                })?;
            }
        }
        self.plan.send_replace(plan);
        Ok(())
    }
}

/// The consumer's handler, shared by a venue's sessions; they all run in one task and none
/// calls it from inside another's call.
struct Shared<H>(Rc<RefCell<H>>);

impl<H: MdHandler> MdHandler for Shared<H> {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.0.borrow_mut().on_md(env);
    }

    fn on_md_with(&mut self, env: Envelope<MdEvent>, out: &mut Outbox) {
        self.0.borrow_mut().on_md_with(env, out);
    }

    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        self.0.borrow_mut().on_tick_to_wire(sample);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().on_epoch_end(key);
    }
}

/// A session running, until it stops.
type Running = Pin<Box<dyn Future<Output = Result<(), SessionError>>>>;

/// One venue's market data, driven by [`MdVenue::run`].
pub struct MdVenue<H> {
    config: MdVenueConfig,
    next_conn: u16,
    handler: Rc<RefCell<H>>,
    plan: watch::Receiver<Vec<EndpointPlan>>,
    /// The open endpoints: their transport and their session's control.
    open: BTreeMap<StreamId, (MdTransport, MdControl)>,
    running: FuturesUnordered<Running>,
    journal: Option<Journal>,
}

impl<H: MdHandler + 'static> MdVenue<H> {
    /// A venue with no endpoint yet, handing events to `handler`, and its control.
    pub fn new(
        config: MdVenueConfig,
        handler: H,
    ) -> Result<(MdVenue<H>, MdVenueControl), SessionError> {
        let caps = config
            .venue
            .caps(&config.cfg)
            .map_err(SessionError::Config)?;
        config.limiter.check(&caps.limits)?;
        config.liveness.rotate_after(caps.md.max_conn_lifetime)?;
        let (tx, plan) = watch::channel(Vec::new());
        let control = MdVenueControl {
            venue: config.venue,
            cfg: config.cfg.clone(),
            specs: config.specs.clone(),
            plan: tx,
        };
        let venue = MdVenue {
            next_conn: config.conns.start,
            config,
            handler: Rc::new(RefCell::new(handler)),
            plan,
            open: BTreeMap::new(),
            running: FuturesUnordered::new(),
            journal: None,
        };
        Ok((venue, control))
    }

    /// Journals every endpoint opened from now on into `journal` ([`MdSession::set_journal`]).
    pub fn set_journal(&mut self, journal: Journal) {
        self.journal = Some(journal);
    }

    /// Applies each plan the control accepts until the control is dropped, then stops every
    /// endpoint. A session's error stops them all and is returned.
    pub async fn run(&mut self) -> Result<(), SessionError> {
        let ran = self.drive().await;
        // Closing every control stops every session; each then ends at once. The first error
        // is the one returned.
        self.open.clear();
        let mut stopped = Ok(());
        while let Some(result) = self.running.next().await {
            stopped = stopped.and(result);
        }
        ran.and(stopped)
    }

    async fn drive(&mut self) -> Result<(), SessionError> {
        loop {
            // The control first: once it has dropped, no session is polled again before the
            // venue stops, even one a plan applied just before the drop started.
            tokio::select! {
                biased;
                changed = self.plan.changed() => match changed {
                    // A plan published just before the control dropped is not applied: the
                    // drop stops the venue rather than opening anything.
                    Ok(()) if self.plan.has_changed().is_err() => return Ok(()),
                    Ok(()) => {
                        let plan = self.plan.borrow_and_update().clone();
                        self.apply(plan)?;
                    }
                    Err(_) => return Ok(()),
                },
                Some(stopped) = self.running.next() => stopped?,
            }
        }
    }

    /// Keeps, opens and closes endpoints by stream so that `plan` is what runs.
    fn apply(&mut self, plan: Vec<EndpointPlan>) -> Result<(), SessionError> {
        let planned: BTreeSet<_> = plan.iter().map(|ep| ep.stream).collect();
        self.open.retain(|stream, _| planned.contains(stream));
        for ep in plan {
            match self.open.get(&ep.stream) {
                Some((transport, control)) if *transport == ep.transport => {
                    control.set_desired(ep.subs);
                }
                _ => self.start(ep)?,
            }
        }
        Ok(())
    }

    /// Opens `ep` under the next connection number, replacing an endpoint of its stream.
    fn start(&mut self, ep: EndpointPlan) -> Result<(), SessionError> {
        if self.next_conn >= self.config.conns.end {
            return Err(SessionError::NoConnectionLeft);
        }
        let conn = self.next_conn;
        self.next_conn += 1;
        let (stream, transport) = (ep.stream, ep.transport.clone());
        let c = &self.config;
        let config = MdSessionConfig {
            venue: c.venue,
            cfg: c.cfg.clone(),
            plan: ep,
            specs: c.specs.clone(),
            connector: c.connector.clone(),
            pacing: c.pacing,
            clock: c.clock.clone(),
            http_max_body: c.http_max_body,
            conn,
            limiter: c.limiter.clone(),
            liveness: c.liveness,
            write_stall: c.write_stall,
        };
        let (mut session, control) = MdSession::new(config, Shared(self.handler.clone()))?;
        if let Some(journal) = &self.journal {
            session.set_journal(journal.clone());
        }
        self.running
            .push(Box::pin(async move { session.run().await }));
        self.open.insert(stream, (transport, control));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{
        ConnKey, ExchTsKind, Feed, FeedHealth, InstrumentId, KernelRxNs, MonoNs, OpKind,
        RateCharge, Stamp, StreamId, TrafficClass, VenueMeta, WallNs, WireSlice,
    };

    /// Records what reaches it, and writes once per event it is handed with an outbox.
    #[derive(Default)]
    struct Recording {
        events: usize,
        ticks: Vec<i64>,
        ended: Vec<ConnKey>,
    }

    impl MdHandler for Recording {
        fn on_md(&mut self, _: Envelope<MdEvent>) {
            self.events += 1;
        }

        fn on_md_with(&mut self, env: Envelope<MdEvent>, out: &mut Outbox) {
            let charge = RateCharge::one(OpKind::Cancel, None);
            out.send(
                WireSlice::plain(b"pull".to_vec()),
                TrafficClass::Safety,
                charge,
            );
            self.on_md(env);
        }

        fn on_tick_to_wire(&mut self, sample: TickToWire) {
            self.ticks.push(sample.nanos);
        }

        fn on_epoch_end(&mut self, key: ConnKey) {
            self.ended.push(key);
        }
    }

    #[test]
    fn the_shared_handler_forwards_events_writes_tick_to_wire_and_epoch_ends() {
        let inner = Rc::new(RefCell::new(Recording::default()));
        let mut shared = Shared(inner.clone());
        let stamp = Stamp {
            ingest_seq: 0,
            kernel_rx: Some(KernelRxNs(1)),
            recv_mono: MonoNs(2),
            recv_wall: WallNs(3),
            conn: ConnKey { conn: 1, epoch: 0 },
        };
        let env = || {
            let meta = VenueMeta {
                exch_ts: None,
                exch_ts_kind: ExchTsKind::Unknown,
                venue_seq: None,
            };
            let ev = MdEvent::Health {
                inst: InstrumentId::new(1),
                feed: Feed::Trades,
                h: FeedHealth::Stale,
            };
            Envelope::new(stamp, meta, ev)
        };
        let mut out = Outbox::new(StreamId(0));
        shared.on_md(env());
        shared.on_md_with(env(), &mut out);
        let stream = StreamId(0);
        shared.on_tick_to_wire(TickToWire {
            stream,
            frame: stamp,
            nanos: 7,
        });
        let mut fx = fbc_core::Effects::new();
        out.drain_into(&mut fx);
        assert_eq!(fx.len(), 1);
        shared.on_epoch_end(stamp.conn);
        let inner = inner.borrow();
        assert_eq!((inner.events, inner.ticks.as_slice()), (2, &[7][..]));
        assert_eq!(inner.ended, [stamp.conn]);
    }
}
