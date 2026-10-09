//! How a venue's order entry answers over the stub server, for the checks that run
//! fbc-runtime's order-entry session against it ([`amend_ack`](super::amend_ack),
//! [`mixed_batch`](super::mixed_batch), [`unknown_on_timeout`](super::unknown_on_timeout),
//! [`resync_after_reconnect`](super::resync_after_reconnect),
//! [`two_phase_ack`](super::two_phase_ack), [`reject_coverage`](super::reject_coverage)).
//!
//! The suite knows no venue's protocol, so the adapter states its replies as typed Rust
//! functions (decisions 0025, 0047): what its order entry is pointed at, the HTTP routes it reads
//! (a login, a resync over REST), the answer to each frame the session writes as an epoch opens,
//! the venue's answer to a request, each of its items answered as the check asks, and how its
//! codec reports a batch's items refused or left unanswered ([`BatchFailures`]).

use core::fmt;
use std::sync::Arc;

use fbc_core::VenueConfig;

use crate::routes::HttpRouter;
use crate::script::{Frame, Responded, Responder};
use crate::server::StubServer;

/// How the stub answers one item of a request (an order of a batch; the one item of a single
/// command).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Answer {
    /// Accepted, as the venue answers an acceptance: a placement resting under a venue id the
    /// answer assigns; an amend accepted with whatever the venue reports it by, the replaced
    /// order's event where `AmendCaps.ack` is `ReplacedEvent` and the reply alone where it is
    /// `RpcReplyOnly`.
    Accept,
    /// Refused by the venue, under a reject code of its own choosing.
    Reject,
    /// Refused by the venue under the reject code given, as the venue's wire spells it: a code
    /// of the fixture's reject table, which [`reject_coverage`](super::reject_coverage) asks
    /// for one placement at a time.
    RejectCode(String),
    /// Left unanswered: nothing is sent for it.
    Silent,
}

type ReplyFn = dyn Fn(&Frame, &[Answer]) -> Responded + Send + Sync;

/// The venue's answer to an order-entry request frame, its items answered as the [`Answer`]s
/// say, in the order the request named them: the frames to send (none where every item is
/// [`Answer::Silent`]), or why the frame cannot be answered (a frame that is no request, or a
/// count of answers that is not its count of items). A function that needs state across
/// requests (the venue ids it assigned) keeps it itself (decision 0047).
#[derive(Clone)]
pub struct Replier(Arc<ReplyFn>);

impl Replier {
    pub fn new(reply: impl Fn(&Frame, &[Answer]) -> Responded + Send + Sync + 'static) -> Replier {
        Replier(Arc::new(reply))
    }

    /// The frames answering `frame`, its items answered as `answers` says.
    pub fn reply(&self, frame: &Frame, answers: &[Answer]) -> Responded {
        (self.0)(frame, answers)
    }

    /// A responder answering the frame it reads as `answers` says.
    pub(crate) fn responder(&self, answers: Vec<Answer>) -> Responder {
        let replier = self.clone();
        Responder::new(move |frame| replier.reply(frame, &answers))
    }
}

impl fmt::Debug for Replier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Replier(..)")
    }
}

/// How the venue's order entry answers over the stub server: what the order-entry checks need
/// to run fbc-runtime's session against it. [`Setup::order_entry`](super::Setup) holds it, built
/// afresh with the rest of the setup for every run, so a function keeping state starts clean.
pub struct OrderEntryStub {
    /// Points the venue's configuration (the setup's) at `stub`: its order-entry socket at
    /// [`StubServer::ws_url`], any REST base at [`StubServer::http_url`].
    pub point: fn(&mut VenueConfig, &StubServer),
    /// The HTTP routes the venue serves the session: a login, a resync over REST. Empty for a
    /// venue whose order entry reads nothing over HTTP.
    pub http: HttpRouter,
    /// The answer to each frame the session writes on its order-entry connection as an epoch
    /// opens, in the order written, until the epoch takes places: its authentication, the
    /// cancel-on-disconnect arm the session sends, and a resync showing the account flat with
    /// nothing of ours resting. Each answers one frame. Every epoch opens alike, the one after
    /// a reconnect too ([`resync_after_reconnect`](super::resync_after_reconnect)): the stub has
    /// closed the connection, so cancel-on-disconnect has cancelled what rested on it.
    pub opening: Vec<Responder>,
    /// The venue's answer to an order-entry request.
    pub reply: Replier,
    /// How the venue's codec reports a batch's items the venue refuses or leaves unanswered,
    /// which [`mixed_batch`](super::mixed_batch) judges it by (decision 0089).
    pub batch: BatchFailures,
}

/// How a venue's codec reports the items of a batch the venue refuses or leaves unanswered:
/// what a venue's batch protocol lets it say of them (decision 0014 item 3). The toy refuses an
/// item with a code and answers each item on its own, so it reports a refused item `Rejected`
/// and an unanswered one `Unknown` at the deadline; Paradex gives a refused item an error
/// message with no code and answers a batch in one list, so its codec reports both `Unknown`
/// from the reply (decision 0069).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct BatchFailures {
    /// What a batch item the venue refuses is reported as.
    pub refused: RefusedItem,
    /// When a batch item the venue's reply leaves unanswered is reported `Unknown`.
    pub unanswered: UnansweredItem,
}

/// What a batch item the venue refuses is reported as.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum RefusedItem {
    /// `SubmitOutcome::Rejected`: the venue's refusal says the item was left undone.
    Rejected,
    /// `SubmitOutcome::Unknown`: the venue's refusal does not say the item was left undone (a
    /// message with no code, decision 0069), so the item is resolved as any `Unknown` is.
    Unknown,
}

/// When a batch item the venue's reply leaves unanswered is reported `Unknown`. The item the
/// check leaves unanswered is the batch's last, so a venue answering a batch in one list, in
/// the order of its items, leaves it out of a shorter list.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum UnansweredItem {
    /// At the request's deadline, never before: the venue answers each item on its own, so a
    /// reply to some items says nothing of the others.
    AtDeadline,
    /// With the reply, before the clock moves: the venue answers a batch in one reply, so an
    /// item that reply leaves out is never answered.
    InReply,
}

impl fmt::Debug for OrderEntryStub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderEntryStub")
            .field("opening", &self.opening.len())
            .finish_non_exhaustive()
    }
}
