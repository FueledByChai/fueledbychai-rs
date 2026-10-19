//! SimVenue hosted by the runtime (FBC-6mf, decision 0104): [`SimPeer`] is the venue end of an
//! in-process order-entry stream, [`fbc_runtime::OrderEntryPeer`], over one [`SimEngine`].

use std::collections::VecDeque;
use std::time::Duration;

use fbc_core::{ConnKey, Envelope, MdEvent, MonoNs, WallNs};
use fbc_runtime::OrderEntryPeer;

use crate::config::SimConfig;
use crate::engine::{Answer, SimEngine};
use crate::wire::Reply;

/// What a [`SimPeer`] was handed and its engine refused, each counted rather than answered: a
/// stream sends no word back for a frame its venue cannot read, and a market-data event the
/// engine's book refuses is the feed's, not the session's.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct PeerCounters {
    /// Order-entry frames the engine could not read, or took while its stream was closed.
    pub refused_frames: u64,
    /// Market-data envelopes the engine's books refused.
    pub refused_envelopes: u64,
    /// Opens of a stream already open and closes of one already closed: a host defect.
    pub refused_lifecycle: u64,
}

/// SimVenue's engine as the venue end of an in-process order-entry stream, which an
/// [`ExecSession`](fbc_runtime::ExecSession) built with
/// [`ExecSession::in_process`](fbc_runtime::ExecSession::in_process) and a
/// [`SimCodec`](crate::SimCodec) of the same [`SimConfig`] writes to (decision 0104). It hands the engine
/// every frame the session writes, every envelope the shard's
/// [`PeerFeed`](fbc_runtime::PeerFeed) gives it, the stream's opening and closing (0097) and
/// each end of a market-data connection epoch ([`SimEngine::end_epoch`]), and gives the
/// session the engine's answers at their due times.
///
/// The simulated venue asks no login, so as the stream opens the peer says it is ready, a
/// frame due [`SimLatency::to_client`](crate::SimLatency) later, which the codec reports as the
/// stream authenticated; the session then arms its cancel-on-disconnect and resyncs as on any
/// venue (decision 0058). A close forgets a greeting not yet due. Answers the engine gave and
/// the session has not read when the stream closes, the venue's cancellations at the close
/// among them, are given on the next connection (0097).
pub struct SimPeer {
    engine: SimEngine,
    to_client: Duration,
    /// The engine's answers not yet given, in the order it gave them.
    answers: VecDeque<Answer>,
    /// When the open stream's greeting is due, until it is given.
    ready: Option<MonoNs>,
    counters: PeerCounters,
}

impl SimPeer {
    /// A peer over a new engine for `config`, its stream closed.
    pub fn new(config: &SimConfig) -> SimPeer {
        SimPeer {
            engine: SimEngine::new(config),
            to_client: config.latency.to_client,
            answers: VecDeque::new(),
            ready: None,
            counters: PeerCounters::default(),
        }
    }

    /// The engine.
    pub fn engine(&self) -> &SimEngine {
        &self.engine
    }

    /// The engine, for what the consumer configures or injects on it ([`SimEngine::inject`],
    /// [`SimEngine::risk_reject`]); answers it gives meanwhile are given at their due times.
    pub fn engine_mut(&mut self) -> &mut SimEngine {
        &mut self.engine
    }

    /// What the peer counted.
    pub fn counters(&self) -> PeerCounters {
        self.counters
    }

    /// Takes what the engine answered into the answers to give.
    fn took(&mut self) {
        self.answers.extend(self.engine.take_answers());
    }
}

impl OrderEntryPeer for SimPeer {
    fn open(&mut self, mono: MonoNs, _wall: WallNs) {
        match self.engine.open(mono) {
            Ok(()) => self.ready = Some(mono + self.to_client),
            Err(_) => self.counters.refused_lifecycle += 1,
        }
        self.took();
    }

    fn close(&mut self, mono: MonoNs, wall: WallNs) {
        if self.engine.close(mono, wall).is_err() {
            self.counters.refused_lifecycle += 1;
        }
        self.ready = None;
        self.took();
    }

    fn on_frame(&mut self, frame: &[u8]) {
        if self.engine.on_order_frame(frame).is_err() {
            self.counters.refused_frames += 1;
        }
    }

    fn on_market(&mut self, env: &Envelope<MdEvent>) {
        if self.engine.on_market(env).is_err() {
            self.counters.refused_envelopes += 1;
        }
        self.took();
    }

    fn on_market_epoch_end(&mut self, key: ConnKey) {
        self.engine.end_epoch(key);
    }

    fn next_due(&self) -> Option<MonoNs> {
        let answer = self.answers.front().map(|a| a.at);
        [self.engine.next_due(), answer, self.ready]
            .into_iter()
            .flatten()
            .min()
    }

    fn take_due(&mut self, now: MonoNs) -> Vec<Vec<u8>> {
        self.engine.advance(now);
        self.took();
        let mut due = Vec::new();
        let mut ready = self.ready.filter(|&at| at <= now);
        while let Some(at) = self.answers.front().map(|a| a.at).filter(|&at| at <= now) {
            // The greeting goes before every answer due at or after it.
            if ready.take_if(|ready| *ready <= at).is_some() {
                self.ready = None;
                due.push(Reply::ready());
            }
            due.extend(self.answers.pop_front().map(|a| a.frame));
        }
        if ready.is_some() {
            self.ready = None;
            due.push(Reply::ready());
        }
        due
    }
}
