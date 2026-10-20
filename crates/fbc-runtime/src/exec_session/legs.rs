//! The order-entry session's other endpoints (FBC-tnsf, decision 0115): every connection
//! [`VenueFactory::plan_exec`](fbc_core::VenueFactory::plan_exec) plans after the first, which
//! carries the orders. Each is a [`Leg`]: its own connection number, epochs, pacing, timers and
//! HTTP requests on a [`Core`] of its own, connected, read and reconnected beside the order
//! connection by the one thread that drives the session, with the session's one codec and
//! handler. What a codec asks for on one connection for another's stream is handed back by the
//! core that took it ([`Core::spilled`]) and executed on that stream's own connection
//! ([`route`]).

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll};

use fbc_core::{
    ConnKey, CtxCall, Effects, ExecCodec, Inbound, KernelRxNs, Stamp, StreamId, dispatch,
};
use fbc_journal::ControlEvent;
use futures_util::{FutureExt, StreamExt};
use tokio::time::Instant;

use super::{ExecHandler, ExecSessionError, Feed, Sink, context, together};
use crate::auth::HandshakeHeaders;
use crate::in_process::Link;
use crate::journal::Journal;
use crate::session_core::{
    Answered, Core, EpochInputs, Wire, WsError, close, next_frame, sleep_or_never, stream_of,
};
use crate::ws::Message;

/// What a connection attempt resolves to: the open link, or `None` when it failed.
type Dialing = Pin<Box<dyn Future<Output = Option<Link>>>>;

/// How many ways a leg's wait can wake: its connection (an attempt due, a dial done or a
/// frame), a timer, an HTTP result.
const LEG_WAKES: u8 = 3;

/// Where one of the session's other endpoints is.
enum State {
    /// Waiting for the pacing's next attempt, once the buckets admit it; `None` when no attempt
    /// will come (the pacing allows none, or the session has run).
    Waiting(Option<Instant>),
    /// An attempt under way, until its deadline.
    Dialing(Dialing, Option<Instant>),
    /// Connected in epoch `key`, on [`Leg::ws`].
    Open(ConnKey),
}

/// One of the session's other endpoints: a connection of its own, on its own core.
pub(super) struct Leg {
    pub(super) core: Core,
    stream: StreamId,
    url: String,
    /// The credential headers every attempt's upgrade carries (decision 0116), shared with the
    /// attempt under way.
    headers: Rc<HandshakeHeaders>,
    state: State,
    /// The open connection, while the leg is [`State::Open`].
    ws: Option<Link>,
    /// Which way the leg's next wait polls first, of [`LEG_WAKES`].
    turn: u8,
    /// The epoch journaled closed as the control dropped, so it is not journaled closed again.
    closed_early: Option<ConnKey>,
}

/// What woke a leg.
pub(super) enum LegWake {
    /// The pacing's next attempt fell due.
    Attempt,
    /// An attempt ended: its link, or `None` when it failed or ran out its deadline.
    Dialed(Option<Link>),
    /// A frame read off epoch `key`, the kernel's receive time of its last packet when known,
    /// or the connection's end.
    Frame(
        Option<Result<Message, WsError>>,
        ConnKey,
        Option<KernelRxNs>,
    ),
    /// The leg's earliest timer fell due.
    Timer,
    /// An HTTP request the leg's codec calls asked for came back.
    Http(Answered),
}

/// The session's other endpoints, in plan order.
#[derive(Default)]
pub(super) struct Legs {
    legs: Vec<Leg>,
    /// Which leg the next wait polls first.
    turn: usize,
}

/// The order connection as a leg's input sees it: its core, its socket, and whether its epoch
/// is still open.
pub(super) struct Primary<'a> {
    pub(super) core: &'a mut Core,
    pub(super) ws: &'a mut Option<Link>,
    pub(super) open: bool,
}

impl Leg {
    pub(super) fn new(core: Core, stream: StreamId, url: String, headers: HandshakeHeaders) -> Leg {
        Leg {
            core,
            stream,
            url,
            headers: Rc::new(headers),
            state: State::Waiting(None),
            ws: None,
            turn: 0,
            closed_early: None,
        }
    }

    /// The epoch the leg is connected in, if it is.
    fn open_key(&self) -> Option<ConnKey> {
        match &self.state {
            State::Open(key) => Some(*key),
            State::Waiting(_) | State::Dialing(..) => None,
        }
    }

    /// Waits for what wakes the leg next: polled fairly from the way after the last wait's
    /// first, so frames that keep arriving starve no timer or result (as the order connection
    /// does). Cancel-safe: an attempt under way is kept across waits.
    async fn next(&mut self) -> LegWake {
        let first = self.turn;
        self.turn = (first + 1) % LEG_WAKES;
        let mut timer = pin!(sleep_or_never(self.core.next_deadline()));
        let http = &mut self.core.http;
        // No result waits: none can be added while the leg waits.
        let mut no_http = false;
        let mut conn = pin!(connection(&mut self.state, &mut self.ws));
        poll_fn(|cx| {
            for n in 0..LEG_WAKES {
                let wake = match (first + n) % LEG_WAKES {
                    0 => conn.as_mut().poll(cx),
                    1 => timer.as_mut().poll(cx).map(|()| LegWake::Timer),
                    _ if no_http => Poll::Pending,
                    _ => match http.poll_next_unpin(cx) {
                        Poll::Ready(Some(done)) => Poll::Ready(LegWake::Http(done)),
                        Poll::Ready(None) => {
                            no_http = true;
                            Poll::Pending
                        }
                        Poll::Pending => Poll::Pending,
                    },
                };
                if wake.is_ready() {
                    return wake;
                }
            }
            Poll::Pending
        })
        .await
    }

    /// The attempt the pacing allows fell due: started when the buckets admit a connection,
    /// otherwise held until they would (decision 0030), as [`Core::connect`] does.
    fn attempt(&mut self) {
        let now = Instant::now();
        if let Err(refused) = self.core.rates.connect(now) {
            self.state = State::Waiting(refused.ready_at);
            return;
        }
        self.core.pacer.attempted(now);
        self.core.counters.attempts += 1;
        let deadline = now.checked_add(self.core.pacer.deadline());
        let (connector, url) = (self.core.connector.clone(), self.url.clone());
        let headers = Rc::clone(&self.headers);
        let dial = async move {
            let ws = connector.websocket_with(&url, &headers).await;
            ws.ok().map(Link::Socket)
        };
        self.state = State::Dialing(Box::pin(dial), deadline);
    }

    /// An attempt that did not open, by its deadline or at all: the next waits on the pacing.
    fn failed(&mut self) {
        let now = Instant::now();
        self.core.counters.failed_attempts += 1;
        self.core.pacer.failed(now);
        self.state = State::Waiting(self.core.pacer.next_attempt(now));
    }

    /// Stamps one message of epoch `key` and decodes it inside the venue's decode scope, as
    /// the order connection's are; the codec's effects go in `fx`. The stamp of a data frame.
    fn decode(
        &mut self,
        feed: &mut Feed<'_>,
        key: ConnKey,
        rx: Option<KernelRxNs>,
        message: &Message,
        fx: &mut Effects,
    ) -> Option<Stamp> {
        let codec = &*feed.codec;
        let redact = |input: Inbound<'_>| codec.redact_inbound(input);
        let (stamp, raw) = self.core.take_in(&redact, key, rx, message)?;
        let mut sink = Sink {
            handler: &mut *feed.handler,
            epochs: &mut self.core.epochs,
            stop: &self.core.stop,
            stamp,
            rpcs: &mut *feed.rpcs,
            orders: feed.orders,
            own: feed.own,
            conn: feed.conn,
        };
        let (codec, stream, specs) = (&mut *feed.codec, self.stream, feed.specs);
        let decoded = dispatch(feed.caps, feed.ns, |scope| {
            codec.on_frame(stream, raw, scope, specs, &mut sink, fx)
        });
        if decoded.is_err() {
            *feed.decode_errors += 1;
            feed.handler.on_decode_error(stamp.conn);
        }
        Some(stamp)
    }

    /// Ends epoch `key`: its buckets forgotten, its HTTP requests' deadlines standing, its close
    /// journaled (unless it was as the control dropped) and the handler told. A drop also tells
    /// the pacing and opens the next epoch, to be connected as the pacing allows; a stop or a
    /// session error leaves no attempt to come.
    fn end(
        &mut self,
        feed: &mut Feed<'_>,
        key: ConnKey,
        dropped: bool,
    ) -> Result<(), ExecSessionError> {
        self.state = State::Waiting(None);
        self.ws = None;
        feed.rpcs.ended(key);
        self.core.rates.closed(key);
        if self.closed_early.take() != Some(key) {
            self.core.control(|| ControlEvent::Closed(key));
        }
        feed.handler.on_epoch_end(key);
        if dropped {
            let now = Instant::now();
            self.core.pacer.dropped(now);
            self.core.epochs.advance()?;
            self.state = State::Waiting(self.core.pacer.next_attempt(now));
        }
        Ok(())
    }
}

/// What the leg's connection has next: its attempt due, its dial done or out of time, or a frame
/// read off `ws`.
async fn connection(state: &mut State, ws: &mut Option<Link>) -> LegWake {
    match state {
        State::Waiting(at) => {
            sleep_or_never(*at).await;
            LegWake::Attempt
        }
        State::Dialing(dial, deadline) => {
            let mut out = pin!(sleep_or_never(*deadline));
            poll_fn(|cx: &mut Context<'_>| {
                if let Poll::Ready(link) = dial.as_mut().poll(cx) {
                    return Poll::Ready(LegWake::Dialed(link));
                }
                out.as_mut().poll(cx).map(|()| LegWake::Dialed(None))
            })
            .await
        }
        State::Open(key) => {
            let frame = next_frame(ws).await;
            LegWake::Frame(frame, *key, ws.as_ref().and_then(Wire::kernel_rx))
        }
    }
}

impl Legs {
    pub(super) fn new(legs: Vec<Leg>) -> Legs {
        Legs { legs, turn: 0 }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.legs.is_empty()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Leg> {
        self.legs.iter()
    }

    /// Every leg journals into `journal` from now on.
    pub(super) fn set_journal(&mut self, journal: &Journal) {
        for leg in &mut self.legs {
            leg.core.journal = Some(journal.clone());
        }
    }

    /// The epoch stream `stream`'s leg is in or waits to open, when one carries it.
    pub(super) fn current_on(&self, stream: StreamId) -> Option<ConnKey> {
        let leg = self.legs.iter().find(|leg| leg.stream == stream)?;
        Some(leg.core.current())
    }

    /// Every leg waits for its first attempt, as the pacing allows from now.
    pub(super) fn start(&mut self) {
        let now = Instant::now();
        for leg in &mut self.legs {
            leg.state = State::Waiting(leg.core.pacer.next_attempt(now));
        }
    }

    /// Waits for what wakes a leg next, polling the legs from the one after the last wait's
    /// first: the leg and what woke it. Pending forever when there is none.
    pub(super) async fn next(&mut self) -> (usize, LegWake) {
        let n = self.legs.len();
        let first = self.turn;
        self.turn = (first + 1) % n.max(1);
        let mut waits: Vec<_> = self
            .legs
            .iter_mut()
            .map(|leg| Box::pin(leg.next()))
            .collect();
        poll_fn(|cx| {
            for k in 0..n {
                let i = (first + k) % n;
                if let Poll::Ready(wake) = waits[i].as_mut().poll(cx) {
                    return Poll::Ready((i, wake));
                }
            }
            Poll::Pending
        })
        .await
    }

    /// As the control dropped: each open leg's epoch journaled closed, then a frame already
    /// waiting on it stamped and journaled in that closed epoch, so it keeps its place in ingest
    /// order and reaches no codec (as the order connection's, Codex P1 on PR #115).
    fn stop(&mut self, codec: &dyn ExecCodec) {
        let redact = |input: Inbound<'_>| codec.redact_inbound(input);
        for leg in &mut self.legs {
            if let Some(key) = leg.open_key() {
                leg.core.control(|| ControlEvent::Closed(key));
                leg.closed_early = Some(key);
                let frame = next_frame(&mut leg.ws).now_or_never();
                let rx = leg.ws.as_ref().and_then(Wire::kernel_rx);
                if let Some(Some(Ok(message))) = frame {
                    let _ = leg.core.take_in(&redact, key, rx, &message);
                }
            }
        }
    }

    /// The run ended: each open leg's epoch ends, journaled closed with a frame waiting on it
    /// stamped behind the close ([`Self::stop`]) and its socket closed (a close frame, if the
    /// buckets admit one) when the session stopped, retired as a drop when it failed, as the
    /// order connection's (0053), the handler told; no leg connects again, and the requests
    /// still in flight are dropped.
    pub(super) fn finish(&mut self, feed: &mut Feed<'_>) -> Result<(), ExecSessionError> {
        let stopped = stopped(feed);
        if stopped {
            self.stop(&*feed.codec);
        }
        for leg in &mut self.legs {
            leg.core.http.clear();
            if let Some(key) = leg.open_key() {
                if let Some(socket) = leg.ws.as_mut().filter(|_| stopped) {
                    close(socket, &leg.core.rates, key);
                }
                leg.end(feed, key, !stopped)?;
            }
            leg.state = State::Waiting(None);
        }
        Ok(())
    }

    /// The epochs a dropped run left open, ended: buckets forgotten, closes journaled (unless
    /// [`Self::stop`] journaled one already, as the run's end panicked stamping a frame behind
    /// it: Codex P2 r4323792811 on PR #188) and `handler` told; with none, as a panic unwinds,
    /// the close is journaled only if the journal is free (as the order connection's, Reviewer
    /// B B2).
    pub(super) fn left(&mut self, mut handler: Option<&mut dyn ExecHandler>) {
        let unwinding = handler.is_none();
        for leg in &mut self.legs {
            if let Some(key) = leg.open_key() {
                (leg.state, leg.ws) = (State::Waiting(None), None);
                leg.core.rates.closed(key);
                let journaled = leg.closed_early.take() == Some(key);
                if !journaled && unwinding {
                    leg.core.closed_unwinding(key);
                } else if !journaled {
                    leg.core.control(|| ControlEvent::Closed(key));
                }
                if let Some(handler) = handler.as_deref_mut() {
                    handler.on_epoch_end(key);
                }
            }
        }
    }
}

/// Takes what woke leg `i`, `wake`, with the session's codec and handler in `feed`: an attempt
/// started, an epoch opened (`on_open` called with exactly the nonces it asks for, its frames
/// charged together as the order connection's are) or an attempt failed; a frame decoded, a
/// timer fired or an HTTP result answered, of the leg's current epoch, its effects executed
/// (those of an ended epoch dropped and counted). A leg whose epoch ended by a drop, a reconnect
/// it asked for or a failed write reconnects through its own pacing while every other
/// connection's epoch stays open. What the codec asked for on another stream is then executed
/// there ([`route`]). Whether the order connection's epoch is still open, when it is given.
pub(super) async fn take(
    feed: &mut Feed<'_>,
    primary: Option<Primary<'_>>,
    legs: &mut Legs,
    i: usize,
    wake: LegWake,
) -> Result<bool, ExecSessionError> {
    // What woke a leg as the control dropped from another thread reaches no codec, and is not
    // stamped: the run's end stamps only what still waits (decision 0115).
    if stopped(feed) {
        return Ok(false);
    }
    let leg = &mut legs.legs[i];
    let open = match wake {
        LegWake::Attempt => {
            leg.attempt();
            true
        }
        LegWake::Dialed(None) => {
            leg.failed();
            true
        }
        LegWake::Dialed(Some(link)) => {
            leg.core.pacer.opened();
            let key = leg.core.current();
            leg.core.control(|| ControlEvent::Opened(key));
            (leg.state, leg.ws) = (State::Open(key), Some(link));
            // A control that dropped as the connection opened stops the session before the
            // codec is told of it, so nothing is reserved or sent.
            !stopped(feed) && {
                let call = CtxCall::Open(leg.stream);
                let (rec, clock) = (feed.rec, feed.clock);
                let ctx = context(&*feed.codec, &mut *feed.nonces, rec, clock, call, None)?;
                let mut fx = Effects::new();
                feed.codec.on_open(leg.stream, &ctx, &mut fx);
                let never = ExecSessionError::OpenNeverFits;
                together(&mut leg.core, feed, &mut leg.ws, key, fx, never).await?
            }
        }
        LegWake::Frame(Some(Ok(message)), key, rx) => {
            let mut fx = Effects::new();
            let origin = leg.decode(feed, key, rx, &message, &mut fx);
            execute(leg, feed, fx, origin).await?
        }
        LegWake::Frame(..) => false,
        LegWake::Timer => match leg.core.take_timer()? {
            Some((stamp, tag)) => {
                let mut fx = Effects::new();
                feed.ring(&mut leg.core.epochs, stamp, tag, &mut fx);
                faulted(feed)?;
                execute(leg, feed, fx, Some(stamp)).await?
            }
            None => true,
        },
        LegWake::Http(done) => {
            let codec = &*feed.codec;
            let redact = |input: Inbound<'_>| codec.redact_inbound(input);
            let stamped = leg.core.stamp_http(Some(&redact), done);
            match leg.core.admit_http(stamped)? {
                Some((stamp, done)) => {
                    let mut fx = Effects::new();
                    feed.answer(&mut leg.core.epochs, stamp, done, &mut fx);
                    execute(leg, feed, fx, Some(stamp)).await?
                }
                None => true,
            }
        }
    };
    if let Some(key) = leg.open_key().filter(|_| !open && !stopped(feed)) {
        leg.end(feed, key, true)?;
    }
    route(feed, primary, legs).await
}

/// Executes `fx` on leg `leg`'s epoch, attributed to the input stamped `origin`; false when the
/// epoch ended.
async fn execute(
    leg: &mut Leg,
    feed: &mut Feed<'_>,
    fx: Effects,
    origin: Option<Stamp>,
) -> Result<bool, ExecSessionError> {
    let open = leg
        .core
        .execute(&mut leg.ws, feed, fx, false, origin)
        .await?;
    faulted(feed)?;
    Ok(open)
}

/// Whether the control has dropped.
fn stopped(feed: &Feed<'_>) -> bool {
    feed.stop.has_changed().is_err()
}

/// The session's error when a timer firing's nonces were mis-reserved.
fn faulted(feed: &mut Feed<'_>) -> Result<(), ExecSessionError> {
    feed.fault.take().map_or(Ok(()), Err)
}

/// Executes, in order, every frame and reconnect a connection's core handed back for another
/// connection's stream ([`Core::spilled`]), the order connection's first: each on the
/// connection its stream names, while that connection's epoch is open, charged there as its
/// turn comes and attributed to the input that asked for it; one for a connection not open
/// (the order connection while it waits to connect, `primary` `None`, or a leg between
/// epochs) is refused and counted where it was asked for. A leg whose epoch it ends reconnects
/// through its own pacing. Once the session has stopped, nothing more is executed. Whether the
/// order connection's epoch is still open (false when it is not given). Once that epoch has
/// ended, in the batch that handed these back or as one of them ends it, the session takes no
/// place before anything more is awaited, so no input taken meanwhile finds it placing on a
/// closed connection (Codex P2 r4323792819 on PR #188).
pub(super) async fn route(
    feed: &mut Feed<'_>,
    mut primary: Option<Primary<'_>>,
    legs: &mut Legs,
) -> Result<bool, ExecSessionError> {
    loop {
        if primary.as_ref().is_some_and(|p| !p.open) {
            feed.orders.set_ready(None);
        }
        if stopped(feed) {
            primary.iter_mut().for_each(|p| p.core.spilled.clear());
            legs.legs
                .iter_mut()
                .for_each(|leg| leg.core.spilled.clear());
            break;
        }
        let spilled = primary.as_mut().and_then(|p| p.core.spilled.pop_front());
        let next = spilled.map(|e| (None, e)).or_else(|| {
            let mut legs = legs.legs.iter_mut().enumerate();
            legs.find_map(|(j, leg)| leg.core.spilled.pop_front().map(|e| (Some(j), e)))
        });
        let Some((from, (effect, origin))) = next else {
            break;
        };
        let stream = stream_of(&effect);
        let mut fx = Effects::new();
        fx.push(effect);
        let to_leg = legs.legs.iter().position(|leg| Some(leg.stream) == stream);
        let to_leg = to_leg.filter(|&j| legs.legs[j].open_key().is_some());
        let refused = match (
            primary.as_mut().filter(|p| Some(p.core.own) == stream),
            to_leg,
        ) {
            (Some(p), _) if p.open => {
                p.open = p.core.execute(p.ws, feed, fx, false, origin).await?;
                faulted(feed)?;
                false
            }
            (None, Some(j)) => {
                let leg = &mut legs.legs[j];
                let open = execute(leg, feed, fx, origin).await?;
                if let Some(key) = leg.open_key().filter(|_| !open && !stopped(feed)) {
                    leg.end(feed, key, true)?;
                }
                false
            }
            _ => true,
        };
        // The connection it names is not open: refused, counted where it was asked for.
        if refused {
            let core = match from {
                Some(j) => &mut legs.legs[j].core,
                None => &mut *primary.as_mut().expect("spilled from it").core,
            };
            core.counters.refused_effects += 1;
        }
    }
    Ok(primary.is_some_and(|p| p.open))
}
