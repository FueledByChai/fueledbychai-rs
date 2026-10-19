//! An order-entry stream with no socket (FBC-6mf, decision 0104): an [`ExecSession`] built with
//! [`ExecSession::in_process`] writes its codec's frames to an [`OrderEntryPeer`] in the same
//! process, and reads the frames the peer answers with, in place of the venue's planned
//! WebSocket. Everything else is the session's as on a socket (0053): one codec for its life,
//! epochs, the arm and resync of every epoch (0058), rate limits, deadlines, journaling (0006)
//! and fbc-oms's authorization of every order command (0057).
//!
//! The peer is the venue end of the stream. It sees, in the shard's ingest order:
//!
//! - each frame the session's codec writes, as written ([`OrderEntryPeer::on_frame`]), and the
//!   stream's opening and closing ([`OrderEntryPeer::open`], [`OrderEntryPeer::close`]), at the
//!   shard clock's stamps, since a peer reads no clock; a run dropped while its epoch is open
//!   closes the stream too, as a dropped socket closes its connection;
//! - each market-data envelope the shard hands its handlers, through a [`PeerFeed`] the
//!   consumer puts in front of its own [`MdHandler`] ([`OrderEntryPeer::on_market`]), and each
//!   end of a market-data connection epoch, after which the books that epoch fed are invalid
//!   until their next snapshot as the runtime's are (FBC-crh, decision 0039;
//!   [`OrderEntryPeer::on_market_epoch_end`]).
//!
//! It answers with frames due at instants on the shard's monotonic clock
//! ([`OrderEntryPeer::next_due`]): the session sleeps until the earliest, then takes what is due
//! ([`OrderEntryPeer::take_due`]) and reads each frame as it reads one off a socket, stamped from
//! the shard's [`IngestClock`](crate::IngestClock) and journaled at that stamp before the codec
//! decodes it. A frame is text when it is UTF-8 and binary otherwise, as the session writes
//! them. Frames the peer gave together and the session had not read when its epoch ended wait
//! with the stream, and the next epoch reads them first. An envelope the feed hands the peer can make an answer due sooner, so the feed wakes
//! the session to look again.
//!
//! Nothing here opens a socket: the stream is a shared handle ([`InProcessStream`]) on one
//! thread, the shard's (design §5.1).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;

use fbc_core::{ConnKey, Envelope, KernelRxNs, MdEvent, MonoNs, WallNs};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite;

use crate::session::{MdHandler, Outbox};
use crate::session_core::{IngestClock, TickToWire, Wire, sleep_or_never};
use crate::ws::{Message, WebSocket};

/// The venue end of an in-process order-entry stream (decision 0104): fed what the session
/// writes and the shard's market data in ingest order, it answers with frames due at instants
/// on the shard's monotonic clock. It reads no clock: every time it is given is a stamp the
/// shard read.
pub trait OrderEntryPeer {
    /// The stream opened at `mono` (`wall` on the wall clock).
    fn open(&mut self, mono: MonoNs, wall: WallNs);

    /// The stream closed at `mono` (`wall` on the wall clock): the session's epoch on it ended,
    /// or its run was dropped.
    fn close(&mut self, mono: MonoNs, wall: WallNs);

    /// A frame the session's codec wrote on the open stream, its bytes as written.
    fn on_frame(&mut self, frame: &[u8]);

    /// One of the shard's market-data envelopes, in ingest order.
    fn on_market(&mut self, env: &Envelope<MdEvent>);

    /// Market-data connection epoch `key` ended: every book it, or an earlier epoch of its
    /// connection, fed last is invalid until its next complete snapshot (decision 0039).
    fn on_market_epoch_end(&mut self, key: ConnKey);

    /// The earliest instant at which time passing changes what the peer would answer: a frame
    /// falling due, or a command it holds arriving at the venue. `None` while nothing waits on
    /// time. Once [`take_due`](OrderEntryPeer::take_due) has run at `now`, it is `None` or later
    /// than `now`.
    fn next_due(&self) -> Option<MonoNs>;

    /// Moves the peer's time to `now` and gives the frames due at the session by then, in the
    /// order it sent them.
    fn take_due(&mut self, now: MonoNs) -> Vec<Vec<u8>>;
}

/// The handle an in-process stream is reached by (decision 0104): the peer, shared by the
/// session that writes to it and the [`PeerFeed`] that hands it market data. Cloning it shares
/// it. One session at a time opens it.
#[derive(Clone)]
pub struct InProcessStream {
    shared: Rc<Shared>,
}

struct Shared {
    peer: Rc<RefCell<dyn OrderEntryPeer>>,
    /// The frames the peer gave and no session has read yet, in order: kept with the stream,
    /// so those still unread when an epoch ends reach the next (Codex r4313036604 on PR #175).
    due: RefCell<VecDeque<Vec<u8>>>,
    /// Woken when the feed handed the peer something that may bring an answer sooner.
    wake: Notify,
}

impl fmt::Debug for InProcessStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InProcessStream")
    }
}

impl InProcessStream {
    /// A stream whose venue end is `peer`; the caller keeps its own handle on it, to read what
    /// the peer holds.
    pub fn new<P: OrderEntryPeer + 'static>(peer: Rc<RefCell<P>>) -> InProcessStream {
        let peer: Rc<RefCell<dyn OrderEntryPeer>> = peer;
        InProcessStream {
            shared: Rc::new(Shared {
                peer,
                due: RefCell::new(VecDeque::new()),
                wake: Notify::new(),
            }),
        }
    }

    /// Hands the peer one market-data envelope, and wakes the session to look again.
    fn on_market(&self, env: &Envelope<MdEvent>) {
        self.shared.peer.borrow_mut().on_market(env);
        self.shared.wake.notify_one();
    }

    /// Tells the peer market-data connection epoch `key` ended, and wakes the session.
    fn on_market_epoch_end(&self, key: ConnKey) {
        self.shared.peer.borrow_mut().on_market_epoch_end(key);
        self.shared.wake.notify_one();
    }
}

/// An [`MdHandler`] that hands every envelope, and every end of a connection epoch, to the
/// peers of its in-process streams first, then to the consumer's handler `H`, in the same call
/// (decision 0104). A consumer running order entry through in-process streams puts it in front
/// of the handler of every market-data session of the shard (a [`BookKeeper`](crate::BookKeeper)
/// included), so each peer sees the shard's market data in ingest order.
pub struct PeerFeed<H> {
    streams: Vec<InProcessStream>,
    handler: H,
}

impl<H: MdHandler> PeerFeed<H> {
    /// Feeds the peers of `streams`, in that order, then `handler`.
    pub fn new(streams: Vec<InProcessStream>, handler: H) -> PeerFeed<H> {
        PeerFeed { streams, handler }
    }

    /// The consumer's handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }
}

impl<H: MdHandler> PeerFeed<H> {
    /// Hands `env` to every peer.
    fn feed(&self, env: &Envelope<MdEvent>) {
        for stream in &self.streams {
            stream.on_market(env);
        }
    }
}

/// Every call reaches the consumer's handler as it reached the feed, after the peers.
impl<H: MdHandler> MdHandler for PeerFeed<H> {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.feed(&env);
        self.handler.on_md(env);
    }

    fn on_md_with(&mut self, env: Envelope<MdEvent>, out: &mut Outbox) {
        self.feed(&env);
        self.handler.on_md_with(env, out);
    }

    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        self.handler.on_tick_to_wire(sample);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        for stream in &self.streams {
            stream.on_market_epoch_end(key);
        }
        self.handler.on_epoch_end(key);
    }
}

/// What an order-entry session's epoch writes and reads on: the socket it opened, or its
/// in-process stream, opened. One lives per epoch, on the stack of the session's run, so the
/// socket is kept unboxed: every frame of a live session is read through it.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Link {
    Socket(WebSocket),
    Peer(PeerEnd),
}

impl Link {
    /// The open stream `stream`, opened now on `clock`.
    pub(crate) fn open(stream: &InProcessStream, clock: &IngestClock) -> Link {
        let (mono, wall) = clock.now();
        stream.shared.peer.borrow_mut().open(mono, wall);
        Link::Peer(PeerEnd {
            stream: stream.clone(),
            clock: clock.clone(),
            open: true,
        })
    }
}

impl Wire for Link {
    async fn send(&mut self, message: Message) -> Result<(), tungstenite::Error> {
        match self {
            Link::Socket(ws) => Wire::send(ws, message).await,
            Link::Peer(peer) => {
                peer.send(message);
                Ok(())
            }
        }
    }

    async fn next(&mut self) -> Option<Result<Message, tungstenite::Error>> {
        match self {
            Link::Socket(ws) => Wire::next(ws).await,
            Link::Peer(peer) => Some(Ok(peer.next().await)),
        }
    }

    fn close_now(&mut self) {
        match self {
            Link::Socket(ws) => Wire::close_now(ws),
            Link::Peer(peer) => peer.close(),
        }
    }

    fn kernel_rx(&self) -> Option<KernelRxNs> {
        match self {
            Link::Socket(ws) => Wire::kernel_rx(ws),
            // No kernel reads an in-process frame.
            Link::Peer(_) => None,
        }
    }
}

/// The session's end of an open in-process stream. Dropped while open, it closes the stream,
/// as a dropped socket closes its connection.
pub(crate) struct PeerEnd {
    stream: InProcessStream,
    clock: IngestClock,
    open: bool,
}

impl PeerEnd {
    /// Hands the peer a data frame the session wrote; a WebSocket control message has no
    /// meaning on the stream and is not handed on.
    fn send(&mut self, message: Message) {
        if message.is_text() || message.is_binary() {
            let frame = message.into_data();
            self.stream.shared.peer.borrow_mut().on_frame(&frame);
        }
    }

    /// The next frame the peer answers with, once it is due. Cancelled, or its epoch ended, it
    /// loses nothing: a frame taken from the peer waits with the stream until a session reads
    /// it.
    async fn next(&mut self) -> Message {
        let shared = &self.stream.shared;
        loop {
            let frame = shared.due.borrow_mut().pop_front();
            if let Some(frame) = frame {
                return match String::from_utf8(frame) {
                    Ok(text) => Message::text(text),
                    Err(e) => Message::binary(e.into_bytes()),
                };
            }
            let (now, _) = self.clock.now();
            let given = shared.peer.borrow_mut().take_due(now);
            if !given.is_empty() {
                shared.due.borrow_mut().extend(given);
                continue;
            }
            // A peer that says something is due by now, having just been given now, would have
            // the session read its clock again at once, for ever: it waits a nanosecond.
            let due = shared.peer.borrow().next_due();
            let due = due.map(|at| at.max(MonoNs(now.0.saturating_add(1))));
            let at = due.and_then(|at| self.clock.instant(at));
            tokio::select! {
                _ = sleep_or_never(at) => {}
                _ = shared.wake.notified() => {}
            }
        }
    }

    /// Closes the stream now, once.
    fn close(&mut self) {
        if std::mem::replace(&mut self.open, false) {
            let (mono, wall) = self.clock.now();
            self.stream.shared.peer.borrow_mut().close(mono, wall);
        }
    }
}

impl Drop for PeerEnd {
    fn drop(&mut self) {
        // A peer borrowed as a panic unwinds is left as it is: a second panic would abort.
        let borrowed = self.stream.shared.peer.try_borrow_mut().is_err();
        if !(std::thread::panicking() && borrowed) {
            self.close();
        }
    }
}
