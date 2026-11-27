//! The glue between one account's order-entry session and fbc-oms (FBC-x69b, decision 0155
//! item 2): an [`ExecHandler`] that routes every event the session hands it into the account's
//! [`Registry`], and the one path the consumer's commands take to the session, so the registry
//! knows which request each answer names. It depends on fbc-core, fbc-oms and fbc-runtime only,
//! and FBC-umqi moved it here whole, with its behaviour. The handler's five entry points are
//! public methods of [`Link`] ([`Link::on_exec`], [`Link::on_epoch_end`],
//! [`Link::on_submitted`], [`Link::on_unprotected`], [`Link::on_decode_error`]), for a
//! consumer's own handler; [`LinkHandler`] hands each call on, then calls the consumer's wake.
//!
//! - **Submitting.** [`Link::submit`] hands an fbc-oms [`Authorization`] to the session's
//!   [`ExecOrders`] and remembers, under the request id it was given, the order each item of
//!   the command names: a placement by its client id, a cancel by its client id (or the one the
//!   registry holds for its venue id), an amend by the order it names with its new price and
//!   total.
//! - **What became of a submission** ([`ExecHandler::on_submitted`]): sent, the placement's send
//!   time ([`Registry::placement_sent`]), the cancel in flight ([`Registry::cancel_sent`]) or the
//!   amend in flight ([`Registry::amend_sent`]) is recorded; not sent, every item's order is told
//!   `NotSent` for its reason (an amend's after it is recorded in flight, as fbc-oms asks of an
//!   amend a gateway took). A place's nonce, and each batch place item's, that the receipt names
//!   for its item is recorded as its order's placement nonce
//!   ([`Registry::placement_nonce_used`], FBC-o0f0, decisions 0057 and 0155), so that, on a venue
//!   that cancels or queries by it, the order can be named by it; an item the receipt names no
//!   nonce for, a place not sent, an amend and a cancel record none, and a refusal is noted as a
//!   problem ([`Note::Problem`]) naming the order and the refusal's kind, never the nonce.
//! - **Events.** A request's outcome goes to the order each of its items names
//!   ([`Registry::on_outcome`]); an outcome naming a request not submitted here is the session's
//!   own (its cancel-on-disconnect arm) and is only noted. An order event is applied under the
//!   venue's sequence with the ingest order as tiebreak ([`Registry::apply_update`]); a fill goes
//!   through the [`FillLedger`] and, accepted, to [`Registry::apply_fill`], and one the ledger
//!   refuses for its time (`BeyondHorizon` or `Untimed`: it may have applied the fill and
//!   forgotten it, or nothing shows the fill newer than its watermark) is also kept in
//!   [`Link::fill_refusals`], for a consumer that stops on one (decision 0132 item 7), until
//!   [`Link::take_fill_refusals`] hands it over (FBC-2dr2): a consumer that runs for long takes
//!   them as it reads them, so the list holds only the refusals since its last take and does not
//!   grow with the run's length, and one that never takes keeps every refusal, as before. The
//!   `Resync*` events are collected into one [`ResyncSnapshot`] and applied at `ResyncEnd`
//!   ([`Registry::resync`]); its `requested_at` is the receive time of the event reporting the
//!   socket authenticated, which comes before the session asks for the resync. A snapshot the
//!   registry refuses is noted as refused ([`Note::ResyncRefused`]): the session opened its
//!   gate at `ResyncEnd` all the same, so the consumer must not place on it.
//!   An order event of an order in our namespace that the registry does not hold is noted as
//!   such ([`Note::Orphan`]): Stop would not cancel it nor the run count its fills. That
//!   includes an order the registry forgot ([`Link::retain_ended`]).
//!   The account's position events are noted as they come, for the consumer to compare with
//!   the position it seeded.
//! - **Asynchronous rejects** (a two-phase venue's risk check refusing what it accepted) are
//!   noted as they come ([`Note::AsyncReject`]) for the consumer to judge.
//! - **Refusals and connection ends.** A venue error naming no request
//!   ([`ExecEvent::UncorrelatedError`]) that comes before the connection authenticated is a
//!   refused login ([`Note::LoginRefused`]: the REST login refused or unanswered, or the auth
//!   frame refused); after it, a problem. Either is kept as the reason its connection ended,
//!   which [`Note::EpochEnd`] carries; with none, the end names what the session saw: its codec
//!   closed the connection, or the socket ended and fbc-runtime does not say why (FBC-dmlw).
//!   A venue error is shown through [`shown`]: a venue's codec may withhold a login refusal's
//!   echoed words itself, but an auth frame's refusal can be the venue's message as received,
//!   and that frame can carry the session token. Each end is also reported to the registry
//!   ([`Registry::order_entry_epoch_ended`], FBC-9svb) at the last `recv_mono` of the envelopes
//!   the session handed the link before it, of any of its connections (the clock's origin,
//!   `MonoNs(0)`, before the first), never earlier than the ended epoch's own last envelope and
//!   that envelope's on a session of one connection, so no instrument cancel-all is built until
//!   a trustworthy resync requested after it. A resync's request time is the latest
//!   authentication of any connection, so the link serves sessions of one order-entry
//!   connection, which is what every venue here plans.
//! - **Unprotected orders** ([`ExecHandler::on_unprotected`], decision 0080): the orders of
//!   ours a connection's resync showed resting from an earlier connection or run, which its
//!   cancel-on-disconnect may not cover, are cancelled at once: a Safety cancel per order, built
//!   by the registry's [`Registry::cancel_many`] through each order's cancel permit, authorized
//!   and submitted through the session's [`ExecOrders`] ([`Link::attach`]), and the request
//!   remembered for the order ([`Link::safety_cancel_of`]). One the registry holds ended or
//!   not at all is not cancelled: decision 0080 queries it (FBC-2cv0), through the same
//!   session as one control command ([`ExecOrders::submit_control`]; decision 0081 lets a
//!   venue's query go as one HTTP request), by its client id and the venue id the session
//!   showed it under, and its placement nonce where the registry holds one, the venue's codec
//!   choosing the reference its `query_refs` declare. The Safety cancels of a notice are
//!   submitted before its queries, so the session sends them first. Its market stays held
//!   until the answer shows it ended: the session's gate reads that answer and releases the
//!   market ([`ExecOrders::may_place_on`]); an answer showing it open, or none, leaves it held.
//!   Where the venue's caps declare no reference the query carries, none is sent and the market
//!   stays held, which is safe. [`Note::Unprotected`] says what was done, each order not cancelled
//!   among its `left` with why, and the link notes each query sent ([`LadderNote::Query`]).
//! - **The Unknown ladder** (FBC-2cv0, decisions 0005's I5 and I9, 0109). The consumer runs
//!   [`Link::ladder`] on its own timer with the timer's time: one pass of fbc-oms's
//!   [`Registry::ladder`] under the [`LadderConfig`] given to [`Link::new`], the one its
//!   resyncs apply. Each query it plans goes to the session as a control command, recorded
//!   with [`Registry::query_sent`] ([`LadderNote::Query`]); each tombstone cancel, by client
//!   id, goes through the registry's authorization and [`Link::submit`] like any cancel of
//!   ours, and is recorded with [`Registry::tombstone_sent`] once the session reports it sent
//!   ([`LadderNote::Tombstone`]). What could not be sent is noted with why
//!   ([`LadderNote::Unsent`]), and a resync the ladder wants is said in
//!   [`LadderPass::resync_wanted`]: the link starts none. An order on the ladder is never
//!   placed or amended again and counts as resting until resolved (fbc-oms holds both).
//! - **Query answers.** A query's answer ([`ExecEvent::QueryResult`]), its request's outcome,
//!   or the session's report that it was not sent goes to [`Registry::on_query_answer`] or
//!   [`Registry::on_query_outcome`] for a ladder query; an unprotected order's is only read.
//!   Each is noted ([`LadderNote::QueryAnswer`]) with what the venue showed and, for the
//!   ladder, what it resolved. A tombstone's outcome that ends its order is noted
//!   ([`LadderNote::TombstoneResolved`]); an order a resync ends Lost is in
//!   [`Note::Resynced`]'s report. A query is kept until it is answered or the consumer
//!   releases it; one the venue acknowledged, which leaves its answer to come and may never
//!   bring it, is forgotten at a [`Link::ladder`] pass once fbc-oms no longer awaits it (a
//!   ladder query) or the intent timeout has passed since it was sent (an unprotected order's),
//!   so a late answer is not read. A tombstone is a cancel the link keeps as its others.
//!   These [`LadderNote`]s are kept apart from [`Link::notes`] ([`Link::ladder_notes`],
//!   until [`Link::take_ladder_notes`] takes them), so a consumer's match on every [`Note`]
//!   keeps compiling; the tombstone's report that it was sent and its answer are ordinary notes.
//! - **Undecodable input** ([`ExecHandler::on_decode_error`]): noted as a problem as it is
//!   read, so a consumer stops trading on what it may have missed.
//! - **Cancels not ours** (FBC-aoh5, the owner's 2026-10-18 mainnet session: a Java instance on
//!   the same account cancelled every order on its ticker it did not place). An order of ours
//!   that an order event ends `Canceled(Requested)` (a user cancel, which a venue may report
//!   without saying who sent it) while no cancel of
//!   ours covers it is noted ([`Note::CancelNotOurs`], with the venue's time on the event) after
//!   its [`Note::Order`]. A cancel of ours covers an order from the moment the session reports
//!   it sent, in flight or answered, until the venue refuses it for that order (or it turns
//!   out not sent after all): a cancel or cancel-many naming it (a replace's, a sweep's, Stop's,
//!   or a Safety cancel of [`ExecHandler::on_unprotected`]), or a cancel-all of its market (or
//!   of the account) submitted while the order was ours and not ended. A refused one (a cancel
//!   refused for its rate, waiting to be sent again) covers nothing from its refusal on, so
//!   another program's cancel of the order, still live, is not ours (Codex r4309154204 on PR
//!   #159); an Unknown one still may land, and still covers. An order a user cancel ends while
//!   only requests of ours not yet answered cover it is held until they are: one accepted, the
//!   cancel was ours; all refused (the venue answering `AlreadyTerminal`, `NotFound` or for its
//!   rate after another program's cancel, its order event read first), it is noted then, after
//!   the refusal's [`Note::Outcome`] (Codex r4310042612 on PR #159). An answer to a cancel-all
//!   for the whole request, or for its item naming no client id, accepts or refuses it for every
//!   order it covers; one naming an order, for that order alone. Submitted but not
//!   yet reported sent, it is only pending and covers nothing: the session decodes a frame
//!   waiting in a turn before it writes what was queued, so another program's cancel read then
//!   is not ours (Codex r4308625182 on PR #159), and one reported not sent wrote nothing (Codex
//!   r4308561813). The session reports a request sent before it writes its first byte, so the
//!   venue's end of an order by our cancel always comes after its cover. Only the event that
//!   ends the order counts (a late duplicate of it is not), so one order is noted at most once.
//!   Once one is noted, [`Link::cancels_not_ours_seen`] holds for the link's life, through
//!   [`Link::forget`] too. Each consumer decides what it means; [`crate::not_ours`] classifies
//!   them under the owner's rule.
//! - **Live orders.** [`Link::live_on`] gives the orders of ours on a market the link holds
//!   live: placed here, shown open by a resync or told unprotected, and not ended in the
//!   registry, in the order the link learnt them. They are what a cancel-all of ours covers, and
//!   what [`crate::sweep`] cancels.
//! - **Requests held** (FBC-mu68, decision 0155 items 2 and 5). The link holds the items of each
//!   request submitted here, and what each cancel cancels, so later answers route to its orders.
//!   It drops a request when the consumer releases it ([`Link::release`]), or at a
//!   [`Link::forget`] once every item of it is answered in full and an earlier forget took the
//!   last note naming it: each forget first drops every request so answered that no note left
//!   names, then takes its notes. An item is answered in full by a final answer to it or to the
//!   whole request (accepted at `AckLevel::Final`, refused, or a report that it was not sent), or
//!   by any answer once its order has ended in the registry: an acceptance at
//!   `AckLevel::Provisional` or `Unknown` keeps it while its order lives, since fbc-oms reads the
//!   answer that may follow (a two-phase venue's final acceptance of a place or an amend, decision
//!   0092; the answer to an amend or a cancel gone Unknown). A request naming no order (a
//!   cancel-all) is answered in full by its first answer, as fbc-oms reads none. An answer naming
//!   an item past the request's items answers none. A request answered in full is still kept
//!   while the link's coverage of cancels not ours waits on an answer to it
//!   ([`Coverage::waits_on`]): an order of ours a user cancel ended while it covered the order
//!   unanswered, held on it, or a live order it covers that the venue has not accepted it for (a
//!   cancel-all answered `Unknown`), whose end by a user cancel would be. Its later answer then
//!   still routes as ours and reaches the coverage, so a refusal notes the order's end as a
//!   cancel not ours (Codex on PR #303). That wait has no bound: a cancel whose held order end
//!   is never answered (answered `Unknown` at its deadline, say, and its connection then ended)
//!   is kept for the link's life, as the held end is, unless the consumer releases it. A
//!   request whose last note a forget takes is
//!   still held until the next forget, so the consumer reads the notes that forget handed it
//!   against the link as it was ([`crate::sweep::Sweep::observe`] reads [`Link::is_cancel_on`]
//!   of a refusal for its rate, which may be its request's last answer). A request with an item
//!   not answered in full is kept, and so is every request while a note naming it is left: a
//!   consumer that never forgets its notes keeps them, and the requests they name, until it
//!   releases them. A later answer to a dropped request is noted as the session's own, as after
//!   a release ([`Note::Outcome`] with `ours` false, which a consumer tells from its session's
//!   cancel-on-disconnect arm by the request ids [`Link::submit`] gave it).
//! - **Cancels.** [`Link::is_cancel_on`] says whether a request submitted here and not yet
//!   released ([`Link::release`]) or dropped ([`Link::forget`]) is a cancel on a market: a
//!   cancel or a cancel-many naming an order of ours on it, each order's market as fbc-oms built
//!   the cancel from its record, or a cancel-all of it, a Safety cancel of
//!   [`ExecHandler::on_unprotected`] included.
//!   [`crate::sweep`] reads it of each refusal for its rate, so another market's cancels make
//!   nothing due in a sweep of one market, though the consumer's hold, which records each refusal
//!   for its rate the consumer reads, another market's included, still delays its resend (decision
//!   0155 item 5).
//! - **Notes.** Everything the consumer may wait on is appended to [`Link::notes`], in order.
//! - **Retention** (FBC-6pdo, decision 0157). [`Link::retain_ended`] runs fbc-oms's retention
//!   pass ([`Registry::retain_ended`]) under the link's order caps, at the consumer's time on
//!   the shard's monotonic clock, under its [`RetentionConfig`]; nothing else here forgets an
//!   order, and a consumer that never calls it keeps every order, as before. The consumer runs
//!   it on a journaled timer, so a closed-loop replay forgets the same orders at the same passes
//!   (decision 0157 item 8), and in this order: [`Link::forget`], then its reading of the notes
//!   that forget handed over, then the pass, so it has read each ended order's notes before the
//!   pass can forget the order. The session reports each command it took over to
//!   [`ExecHandler::on_submitted`] before it drops the command, and the link records the report
//!   there, so the handles the command holds pin the orders it names until the registry owes
//!   their answers (decision 0157 item 3d).
//!   The link's snapshots take the receive time of the socket's authentication as their
//!   `requested_at`, so the configuration's resync bound must cover the time from that
//!   authentication to the `ResyncEnd` the link applies, which is the sum of three times: the
//!   rest of the turn that took the authentication, up to the session's ask for the resync (the
//!   handler's work on that input; the codec's frames and the arm the session writes, and the
//!   frames the effects below add, each write bounded by the session's `WriteStall` window; the
//!   HTTP results and timer firings the session takes while one of those writes waits, which
//!   reach the codec and the handler; and, on a shard host, the host's other work while a write
//!   waits, its other members' turns and the consumer's decide pass, which can hold the session
//!   past that window); the session's `ResyncDeadline` (FBC-nwpr); and how long the turn under
//!   way when that deadline passes may still run before its check, with the HTTP results and
//!   timer firings it takes while one of its writes waits, since a `ResyncEnd` it hands on
//!   first, from its input or from one of those results, ends the resync and keeps the epoch
//!   open (and, on a shard host, the host's other work it waits on). A bound set short may
//!   register a forgotten order as an orphan, which fails closed.
//!   A late event naming a forgotten order is noted as one of an order never held: an update
//!   under our client id as [`Note::Orphan`], unless its venue id routes it to an order the
//!   registry holds, when it is noted [`Note::Order`] with [`Routed::Ours`]; an update naming a
//!   venue id alone as [`Note::Order`] with [`Routed::Untracked`], unless that id routes it to
//!   an order the registry holds; and a fill under its client id that the registry counts after
//!   its market's seed (fbc-oms's `OursUntracked`) as [`Note::Fill`] with `unexplained` set.
//!   An outcome naming a forgotten order of a request the link still holds is noted as ours,
//!   with no problem: fbc-oms answers it as a terminal record's.
//!
//! - **The clock** (decision 0155 item 3). The link reads no clock of its own. Times on the
//!   shard's monotonic clock are taken from each event's stamp; a submission's send time, which
//!   carries no stamp, from [`Link::now`]: the shard clock, its
//!   [`IngestClock`](fbc_runtime::IngestClock), of the session whose [`ExecOrders`] the link was
//!   last attached to ([`Link::attach`]) or submitted through ([`Link::submit`]), read through
//!   [`ExecOrders::now`] as that session reads it. A submission is timed on the clock last
//!   taken, since its [`SubmitHandle`] names no session: the reporting session's whenever the
//!   link's sessions share one clock, as every sample's do. An [`ExecOrders`] on another clock
//!   is noted as a problem ([`Note::Problem`]) and its clock read from then on; its submission
//!   still goes, so no cancel is held back. In a closed-loop replay that clock is
//!   [`IngestClock::replayed`](fbc_runtime::IngestClock::replayed), so two replays of one
//!   journal record the same send times (decision 0155 item 4). A fill's venue time is its
//!   aligned wall time, unadjusted (no clock-skew estimate yet), which a replayed fill's
//!   admission and a fill's placement against a seed read (fbc-oms's resync; decisions 0055,
//!   0095 and 0155 item 3).

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use fbc_core::{
    AccountKey, AckLevel, CancelReason, CancelScope, CidMatch, ClientOrderId, ConnKey, ConnState,
    EncodeReceipt, Envelope, ExchNs, ExecEvent, InstrumentId, ItemRef, Lots, MonoNs, NotSentReason,
    OpKind, OrderCaps, OrderRef, QueryAnswer, QueryOrder, Reject, RpcId, Side, SignedLots,
    SubmitHandle, SubmitOutcome, Ticks, VenueCommand, VenueOrderId, VenueOrderSnapshot,
    VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, Applied, Authorization, ControlCommand, FillLedger, FillRouted, FillTime,
    LadderConfig, LadderResolution, LadderStep, OrdState, OrderKey, OrderOp, OutcomeApplied,
    Registry, ResyncReport, ResyncSnapshot, Retained, RetentionConfig, Routed, TerminalKind,
};
use fbc_runtime::{ExecHandler, ExecOrders};

/// Something the session reported, as the glue saw it. A resync's report is the largest note;
/// the notes are the consumer's records, kept whole rather than boxed (FBC-5orj grew the report by
/// the orders shown without a client id).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Note {
    /// A stream's state changed.
    Conn(ConnState),
    /// The outcome of a request: `ours` when submitted through [`Link::submit`] and still held
    /// ([`Link::release`], [`Link::forget`]), otherwise the session's own request (its
    /// cancel-on-disconnect arm) or a later answer to one of ours the link no longer holds, which
    /// a consumer tells apart by the request ids [`Link::submit`] gave it, not by its releases.
    Outcome {
        rpc: RpcId,
        /// The item it answers, `None` for the whole request.
        item: Option<u16>,
        outcome: SubmitOutcome,
        ours: bool,
    },
    /// A submission was sent, or not and why.
    Submitted {
        rpc: RpcId,
        sent: Result<(), NotSentReason>,
    },
    /// An order event, where it went: anything but an order in our namespace that the
    /// registry does not hold ([`Note::Orphan`]).
    Order(Routed),
    /// An order event of an order in our namespace that the registry does not hold: an earlier
    /// run's that the resync did not show, or one the registry forgot (decision 0157). Its
    /// client id and venue id.
    Orphan {
        cid: ClientOrderId,
        vid: Option<VenueOrderId>,
    },
    /// A fill, where it went (or why the ledger did not apply it): `unexplained` unless it was
    /// counted on an order of ours, held by the resync's position, or one the ledger already
    /// held (a duplicate, or a replay the starting position holds); `ours` when it was applied,
    /// counted on an order of ours and the inventory.
    Fill {
        seen: String,
        unexplained: bool,
        ours: bool,
    },
    /// The account's position in an instrument, as the venue's position stream reported it.
    Position { inst: InstrumentId, qty: SignedLots },
    /// A resync was applied: its report, and the positions and open orders it showed.
    Resynced {
        report: ResyncReport,
        snapshot: ResyncSnapshot,
    },
    /// A resync the registry refused, and why: nothing of it was applied.
    ResyncRefused(String),
    /// A refusal that came after the venue accepted the request (a two-phase venue's risk
    /// check): of what, which order, and why. Each consumer decides what it means (RB-elg7-1 on
    /// PR #149): one of its own post-only places closed POST_ONLY_WOULD_CROSS is an ordinary
    /// end for a quoting loop, a reason to Stop for a one-order round trip.
    AsyncReject {
        target: OrderRef,
        op: OpKind,
        reject: Reject,
    },
    /// A refusal from the venue that no outcome carries, or something the glue could not apply.
    Problem(String),
    /// The login was refused before the connection authenticated (module documentation): the
    /// venue's error as [`described`] shows it.
    LoginRefused(String),
    /// A connection epoch ended, and why, as far as the session reported it.
    EpochEnd { key: ConnKey, why: String },
    /// The session told the orders of ours its connection's cancel-on-disconnect may not cover
    /// (decision 0080): each with its market, the Safety cancel requests submitted for them,
    /// and why any was not cancelled.
    Unprotected {
        told: Vec<(ClientOrderId, InstrumentId)>,
        rpcs: Vec<RpcId>,
        left: Vec<String>,
    },
    /// Our order `cid` (on `side`) was just ended by a user cancel (`Canceled(Requested)`) that
    /// no cancel of ours covers (module documentation, FBC-aoh5): another program on the
    /// account, or a person in the venue's UI. Noted right after its [`Note::Order`], with the
    /// venue's time on the event (`at`, `None` when it sent none): one action of the other
    /// program's (a reconcile, a cancel-all) ends what it ends at nearly one venue time.
    CancelNotOurs {
        cid: ClientOrderId,
        side: Side,
        at: Option<ExchNs>,
    },
}

/// What the link sent for fbc-oms's Unknown ladder and for the query of an unprotected order,
/// and what answered it ([`Link::ladder_notes`], module documentation), apart from
/// [`Link::notes`] so that a consumer's match on every [`Note`] keeps compiling.
#[derive(Clone, Debug, PartialEq)]
pub enum LadderNote {
    /// A query of our order `cid` was submitted under `rpc`, for the Unknown ladder or an
    /// unprotected order (module documentation).
    Query {
        cid: ClientOrderId,
        rpc: RpcId,
        of: Queried,
    },
    /// The query `rpc` of our order `cid` was answered: what the venue showed, or the request's
    /// outcome; for a ladder query, what fbc-oms's ladder did with it (`None` for an unprotected
    /// order's, whose market the session's gate releases on an answer showing it ended).
    QueryAnswer {
        cid: ClientOrderId,
        rpc: RpcId,
        of: Queried,
        read: QueryRead,
        resolution: Option<LadderResolution>,
    },
    /// The Unknown ladder's tombstone cancel of our order `cid` was submitted under `rpc`.
    Tombstone { cid: ClientOrderId, rpc: RpcId },
    /// The tombstone cancel `rpc` of our order `cid` was answered and ended it, in `state`:
    /// Canceled once accepted for good, Lost once refused as already ended.
    TombstoneResolved {
        cid: ClientOrderId,
        rpc: RpcId,
        state: OrdState,
    },
    /// What a ladder pass planned for our order `cid` and could not send, and why.
    Unsent {
        cid: ClientOrderId,
        what: LadderCommand,
        why: String,
    },
}

/// What a query the link sent is for (module documentation).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Queried {
    /// The Unknown ladder's query of an order on it ([`Link::ladder`]).
    Ladder,
    /// An unprotected order the registry holds ended or not at all ([`Link::on_unprotected`]).
    Unprotected,
}

/// What answered a query ([`LadderNote::QueryAnswer`]).
#[derive(Clone, Debug, PartialEq)]
pub enum QueryRead {
    /// The venue showed the order in this state.
    Shown(VenueOrderState),
    /// The venue does not know the order.
    NotFound,
    /// The request's outcome with no answer: not sent, refused, accepted with the answer still
    /// to come, or unanswered (`Unknown`, never sent again).
    Outcome(SubmitOutcome),
}

/// A command of a ladder pass ([`LadderNote::Unsent`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LadderCommand {
    Query,
    Tombstone,
}

/// What one pass of the Unknown ladder ([`Link::ladder`]) did, each order by client id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LadderPass {
    /// The orders this pass put on the ladder: a placement or a command in flight past the
    /// intent timeout.
    pub escalated: Vec<ClientOrderId>,
    /// The queries submitted, with their requests.
    pub queries: Vec<(ClientOrderId, RpcId)>,
    /// The tombstone cancels submitted, with their requests.
    pub tombstones: Vec<(ClientOrderId, RpcId)>,
    /// What was planned and not submitted, and why, each also noted
    /// ([`LadderNote::Unsent`]): a query or tombstone the session refused, a tombstone the
    /// registry did not authorize, or one the venue's single cancel cannot name by client id.
    pub unsent: Vec<(ClientOrderId, LadderCommand, String)>,
    /// An order on the ladder waits for a resync: the link starts none, so the consumer asks
    /// for one.
    pub resync_wanted: bool,
}

/// A query the link sent ([`LadderNote::Query`]): its order, what for, when it was sent on the
/// session's shard clock, and whether the venue acknowledged it with its answer still to come.
#[derive(Copy, Clone, Debug)]
struct Query {
    cid: ClientOrderId,
    of: Queried,
    sent: Option<MonoNs>,
    accepted: bool,
}

/// Why a connection ended when the session reported no venue error with it: its codec
/// closed it ([`CODEC_CLOSED`]), or nothing the handler is told says ([`ENDED_UNSAID`]: the
/// codec asks for a reconnect with no event when a login cannot be signed; FBC-dmlw).
pub const CODEC_CLOSED: &str = "the session closed it to log in again, with no venue error";
pub const ENDED_UNSAID: &str = concat!(
    "the socket closed or failed, a write stalled, or the login could not be signed ",
    "(fbc-runtime does not say which yet, FBC-dmlw)"
);

/// The longest word of a venue's text [`shown`] shows: a longer one could hold an echoed
/// credential (a session token, a key, a signature, an account), so it is withheld, as a
/// venue's codec may withhold a login refusal's words itself.
///
/// This limit and the three below stay fixed, as do [`withheld`]'s rules: they keep an echoed
/// credential out of what is printed (decision 0009), so no consumer tunes them (decision 0155
/// item 8).
const LONGEST_SHOWN_WORD: usize = 15;
/// The longest number [`shown`] shows (an HTTP status, a JSON-RPC code).
const LONGEST_NUMBER: usize = 6;
/// The most characters of a venue's text [`shown`] shows.
const MOST_SHOWN: usize = 200;
/// The longest error code [`described`] shows.
const LONGEST_CODE: usize = 64;

/// Whether `word` could hold a secret: longer than [`LONGEST_SHOWN_WORD`], holding anything but
/// printable ASCII, or holding a digit unless it is a short number (an optional '-' and at most
/// [`LONGEST_NUMBER`] digits), or a run of six or more hex digits. A word of capitals joined by
/// '_' (an error code's shape, `STARKNET_SIGNATURE_VERIFICATION_FAILED`) is judged part by
/// part: no key, signature or token is written in capitals alone.
fn withheld(word: &str) -> bool {
    let code = word.contains('_')
        && word.bytes().all(|b| b.is_ascii_uppercase() || b == b'_')
        && !word.split('_').any(str::is_empty);
    if code {
        return word.split('_').any(withheld);
    }
    let digits = word.strip_prefix('-').unwrap_or(word);
    let number = !digits.is_empty()
        && digits.len() <= LONGEST_NUMBER
        && digits.bytes().all(|b| b.is_ascii_digit());
    word.len() > LONGEST_SHOWN_WORD
        || word.bytes().any(|b| !b.is_ascii_graphic())
        || (word.bytes().any(|b| b.is_ascii_digit()) && !number)
        || (word.len() >= 6 && word.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// `text` as the run prints a venue's words: each word that could hold a secret ([`withheld`])
/// replaced by `<withheld>`, control characters and whitespace as spaces, cut at [`MOST_SHOWN`]
/// characters. Words are split at whitespace and at punctuation that cannot sit inside a
/// token; a token's '.', '-' and '_' keep it one (long) word.
pub fn shown(text: &str) -> String {
    let (mut out, mut word) = (String::new(), String::new());
    let flush = |word: &mut String, out: &mut String| {
        if !word.is_empty() {
            out.push_str(if withheld(word) { "<withheld>" } else { word });
        }
        word.clear();
    };
    for c in text.chars() {
        if c.is_whitespace() || c.is_control() {
            flush(&mut word, &mut out);
            out.push(' ');
        } else if matches!(
            c,
            ',' | ';'
                | ':'
                | '"'
                | '\''
                | '`'
                | '('
                | ')'
                | '['
                | ']'
                | '{'
                | '}'
                | '<'
                | '>'
                | '='
                | '|'
        ) {
            flush(&mut word, &mut out);
            out.push(c);
        } else {
            word.push(c);
        }
    }
    flush(&mut word, &mut out);
    match out.char_indices().nth(MOST_SHOWN) {
        Some((at, _)) => format!("{}...", &out[..at]),
        None => out,
    }
}

/// An asynchronous reject ([`Note::AsyncReject`]) as a consumer prints it: the operation, the
/// kind, and the venue's reason as [`described`] shows it.
pub fn async_reject(op: OpKind, reject: &Reject) -> String {
    format!(
        "asynchronous reject of a {op:?}: {:?} ({})",
        reject.kind,
        described(reject)
    )
}

/// A venue error as the run prints it: its code (when plain: letters, digits, '_' and '-', no
/// part withheld; `<withheld>` otherwise) unless its text already names it, then its text,
/// both through [`shown`].
pub fn described(reject: &Reject) -> String {
    let raw = shown(&reject.raw);
    let Some(code) = reject.venue_code.as_deref() else {
        return raw;
    };
    let plain = code.len() <= LONGEST_CODE
        && !code.is_empty()
        && code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        && !code.split('_').any(withheld);
    if !plain {
        return format!("<withheld>: {raw}");
    }
    if raw.contains(code) {
        raw
    } else {
        format!("{code}: {raw}")
    }
}

/// Which orders of ours the cancels of ours submitted here cover (module documentation): each
/// order with the requests that cover it, once the session reports each sent. A request
/// submitted but not yet reported sent is pending and covers nothing: the session may decode
/// another program's cancel of its order before it writes it (Codex r4308625182 on PR #159);
/// one reported not sent never covers (no byte of it was written: Codex r4308561813).
///
/// An order that ends `Canceled(Requested)` while only requests not yet answered cover it is
/// held ([`Coverage::hold`]) until they are: the venue may refuse ours after another program's
/// cancel ended the order (`AlreadyTerminal`, `NotFound`, or for its rate), its order event read
/// first. One accepted makes the cancel ours; all refused, the cancel was not ours, and
/// [`Coverage::refused`] hands the held order back to be noted then (Codex r4310042612 on PR
/// #159). The link keeps each request the coverage waits on an answer to ([`Coverage::waits_on`])
/// through [`Link::forget`], so that answer still reaches it (Codex on PR #303).
#[derive(Debug, Default)]
pub struct Coverage {
    /// Each order covered, with the requests covering it and whether each was accepted.
    covered: HashMap<ClientOrderId, HashMap<RpcId, bool>>,
    pending: HashMap<RpcId, Vec<ClientOrderId>>,
    held: HashMap<ClientOrderId, Held>,
}

/// An order of ours ended by a user cancel while only unanswered requests of ours covered it
/// ([`Coverage::hold`]): those requests, and the order's side and the venue's time on its end.
#[derive(Debug)]
struct Held {
    rpcs: HashSet<RpcId>,
    side: Side,
    at: Option<ExchNs>,
}

/// A held order whose every covering request was refused: a cancel not ours after all.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Released {
    pub cid: ClientOrderId,
    pub side: Side,
    pub at: Option<ExchNs>,
}

impl Coverage {
    /// Request `rpc` was submitted to cancel `cids` (a cancel-all: every live order of ours on
    /// its market then): pending until the session reports it sent.
    pub fn submitted(&mut self, rpc: RpcId, cids: impl IntoIterator<Item = ClientOrderId>) {
        self.pending.entry(rpc).or_default().extend(cids);
    }

    /// The session reported request `rpc` sent (`true`: it covers its orders from now on) or
    /// not sent (`false`: it never covers them).
    pub fn reported(&mut self, rpc: RpcId, sent: bool) {
        let Some(cids) = self.pending.remove(&rpc) else {
            return;
        };
        if sent {
            for cid in cids {
                self.covered.entry(cid).or_default().insert(rpc, false);
            }
        }
    }

    /// The venue accepted request `rpc` for order `cid` (`None`: for every order it covers): a
    /// cancel of ours, so an order it covers that a user cancel ends was cancelled by us, and a
    /// held one is ours ([`Coverage::hold`]).
    pub fn accepted(&mut self, rpc: RpcId, cid: Option<ClientOrderId>) {
        for (covered, rpcs) in &mut self.covered {
            if cid.is_none_or(|c| c == *covered)
                && let Some(accepted) = rpcs.get_mut(&rpc)
            {
                *accepted = true;
            }
        }
        self.held
            .retain(|held, h| !(cid.is_none_or(|c| c == *held) && h.rpcs.contains(&rpc)));
    }

    /// Our order `cid` on `side` was ended by a user cancel the venue stamped `at` while a
    /// request of ours covers it: `true` when none of them was accepted yet, so the order is
    /// held until they are answered ([`Coverage::accepted`], [`Coverage::refused`]); `false`
    /// when one was accepted (the cancel was ours) or none covers it.
    pub fn hold(&mut self, cid: ClientOrderId, side: Side, at: Option<ExchNs>) -> bool {
        let Some(rpcs) = self.covered.get(&cid) else {
            return false;
        };
        if rpcs.values().any(|accepted| *accepted) {
            return false;
        }
        let rpcs = rpcs.keys().copied().collect();
        self.held.insert(cid, Held { rpcs, side, at });
        true
    }

    /// The venue refused request `rpc`, or it was not sent after all (its write failed), for
    /// order `cid` (`None`: for every order it covers): it no longer covers it, so another
    /// program's cancel of that order, still live, while ours waits to be sent again is not ours
    /// (Codex r4309154204 / RB-aoh5-4 on PR #159). Another request of ours may still cover it.
    /// The orders held ([`Coverage::hold`]) that no request of ours covers any more: ended by a
    /// cancel not ours, read before the refusal (Codex r4310042612).
    pub fn refused(&mut self, rpc: RpcId, cid: Option<ClientOrderId>) -> Vec<Released> {
        let mut released = Vec::new();
        self.held.retain(|held, h| {
            if cid.is_none_or(|c| c == *held) && h.rpcs.remove(&rpc) && h.rpcs.is_empty() {
                released.push(Released {
                    cid: *held,
                    side: h.side,
                    at: h.at,
                });
                return false;
            }
            true
        });
        released.sort_by_key(|r| r.cid);
        let uncover = |rpcs: &mut HashMap<RpcId, bool>| {
            rpcs.remove(&rpc);
            !rpcs.is_empty()
        };
        match cid {
            Some(cid) => {
                if let Some(rpcs) = self.covered.get_mut(&cid)
                    && !uncover(rpcs)
                {
                    self.covered.remove(&cid);
                }
                if let Some(cids) = self.pending.get_mut(&rpc) {
                    cids.retain(|c| *c != cid);
                }
            }
            None => {
                self.covered.retain(|_, rpcs| uncover(rpcs));
                self.pending.remove(&rpc);
            }
        }
        released
    }

    /// Whether the coverage still waits on an answer to request `rpc`, so the link must keep
    /// routing its answers as ours ([`Link::forget`]): an order end held on it
    /// ([`Coverage::hold`]); an order `live` takes that it covers and the venue has not yet
    /// accepted it for, whose end by a user cancel would be held on it (a cancel-all answered
    /// `Unknown`); or not yet reported sent, so the report finds the request it covers for (a
    /// session reports a request sent before it writes it, so this holds only for a request
    /// answered before its report). An order end held on it waits for as long as no answer
    /// comes, with no bound.
    pub fn waits_on(&self, rpc: RpcId, live: impl Fn(&ClientOrderId) -> bool) -> bool {
        self.pending.contains_key(&rpc)
            || self.held.values().any(|h| h.rpcs.contains(&rpc))
            || self
                .covered
                .iter()
                .any(|(cid, rpcs)| rpcs.get(&rpc) == Some(&false) && live(cid))
    }

    /// Whether a request of ours reported sent covers `cid`.
    pub fn covers(&self, cid: ClientOrderId) -> bool {
        self.covered.contains_key(&cid)
    }

    /// Keeps the orders `keep` takes (the live ones), forgetting the rest, pending or covered;
    /// the held ones, ended, stay until their requests are answered.
    pub fn retain(&mut self, keep: impl Fn(&ClientOrderId) -> bool) {
        self.covered.retain(|cid, _| keep(cid));
        self.pending.retain(|_, cids| {
            cids.retain(|cid| keep(cid));
            !cids.is_empty()
        });
    }
}

/// One item of a submitted command, as the registry knows it.
#[derive(Copy, Clone, Debug)]
enum Item {
    Place(ClientOrderId),
    Cancel(ClientOrderId),
    /// An amend of the order to a new price and total quantity.
    Amend {
        cid: ClientOrderId,
        px: Ticks,
        qty: Lots,
    },
}

/// What a cancel submitted here cancels ([`Link::is_cancel_on`]).
#[derive(Clone, Debug)]
enum Cancels {
    /// Explicit cancels (a cancel or a cancel-many): the market of each order they name, as
    /// fbc-oms built the cancel from its record.
    Orders(Vec<InstrumentId>),
    /// A cancel-all of this scope.
    All(CancelScope),
}

impl Cancels {
    /// Whether they cancel on `inst`. fbc-oms authorizes no account cancel-all (decision 0045);
    /// one would cancel on every market.
    fn on(&self, inst: InstrumentId) -> bool {
        match self {
            Cancels::Orders(markets) => markets.contains(&inst),
            Cancels::All(scope) => {
                [CancelScope::Account, CancelScope::Instrument(inst)].contains(scope)
            }
        }
    }
}

/// How far one item of a request submitted here was answered ([`Link::forget`]), each answer
/// raising it, never lowering it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Answered {
    /// No answer yet.
    Not,
    /// An answer fbc-oms may read a later one after: an acceptance at `AckLevel::Provisional`
    /// (a two-phase venue's final acceptance, or an amend's, may follow) or `Unknown`.
    Open,
    /// Answered for good: accepted at `AckLevel::Final`, refused or not sent.
    Final,
}

/// A fill the ledger refused for its time ([`Link::fill_refusals`], [`Link::take_fill_refusals`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FillRefusal {
    /// The ledger's admission, by its name: `BeyondHorizon` or `Untimed`.
    pub admission: &'static str,
    /// When the fill arrived, on the shard's monotonic clock.
    pub at: MonoNs,
}

/// The registry, the fill ledger and what was submitted (module documentation).
pub struct Link {
    reg: Registry,
    ledger: FillLedger,
    /// Each fill the ledger refused for its time, in order, since [`Link::take_fill_refusals`]
    /// last took them.
    fill_refusals: Vec<FillRefusal>,
    caps: OrderCaps,
    ladder: LadderConfig,
    /// The items of each request submitted here.
    sent: HashMap<RpcId, Vec<Item>>,
    /// What each cancel submitted here cancels, until it is released or dropped
    /// ([`Link::is_cancel_on`]).
    cancels: HashMap<RpcId, Cancels>,
    /// How far each item of each request submitted here was answered (one for a request naming
    /// no order): what [`Link::forget`] reads to drop a request once its notes are gone.
    answered: HashMap<RpcId, Vec<Answered>>,
    /// The resync being collected, from `ResyncBegin` to `ResyncEnd`.
    collecting: Option<ResyncSnapshot>,
    /// When the socket was last reported authenticated.
    authenticated_at: Option<MonoNs>,
    /// The receive time of the last envelope the session handed the link.
    last_recv: Option<MonoNs>,
    /// The current connection: whether it authenticated, whether the codec closed it, and the
    /// last venue error naming no request it reported (its end's reason).
    epoch_authenticated: bool,
    epoch_closed: bool,
    epoch_error: Option<String>,
    /// The orders of the session the link was last attached to or submitted through: whose
    /// shard clock [`Link::now`] reads.
    clock: Option<ExecOrders>,
    notes: Vec<Note>,
    /// The places and cancels sent that notes [`Link::forget`] took counted.
    forgotten: (usize, usize),
    /// The session's orders and the account, where Safety cancels go ([`Link::attach`]).
    session: Option<(ExecOrders, AccountKey)>,
    /// The Safety cancel request last submitted for each unprotected order.
    safety: HashMap<ClientOrderId, RpcId>,
    /// The orders of ours that have not ended that the link knows (placed here, or shown by a
    /// resync), with their market and when the link learnt each, counted from 0: what a
    /// cancel-all of ours covers.
    live: HashMap<ClientOrderId, (InstrumentId, u64)>,
    /// How many orders of ours the link has learnt: the next one's count in `live`.
    learnt: u64,
    /// The orders of ours a cancel of ours submitted here covers (module documentation): an
    /// order that ends `Canceled(Requested)` outside it was cancelled by someone else.
    covered: Coverage,
    /// Whether a [`Note::CancelNotOurs`] was ever pushed ([`Link::cancels_not_ours_seen`]).
    not_ours_seen: bool,
    /// The queries submitted here, until answered or released.
    queries: HashMap<RpcId, Query>,
    /// What the link sent for the ladder and the queries, and what answered it, in order, since
    /// [`Link::take_ladder_notes`] last took them.
    ladder_notes: Vec<LadderNote>,
    /// The tombstone cancels among the requests submitted here.
    tombstones: HashSet<RpcId>,
}

impl Link {
    /// The glue over `reg`, its fills deduplicated by `ledger`, resyncs applied under the venue's
    /// order caps `caps` and `ladder`. It reads no clock until it is attached to a session or
    /// submits through one ([`Link::now`]).
    pub fn new(reg: Registry, ledger: FillLedger, caps: OrderCaps, ladder: LadderConfig) -> Link {
        Link {
            reg,
            ledger,
            fill_refusals: Vec::new(),
            caps,
            ladder,
            sent: HashMap::new(),
            cancels: HashMap::new(),
            answered: HashMap::new(),
            collecting: None,
            authenticated_at: None,
            last_recv: None,
            epoch_authenticated: false,
            epoch_closed: false,
            epoch_error: None,
            clock: None,
            notes: Vec::new(),
            forgotten: (0, 0),
            session: None,
            safety: HashMap::new(),
            live: HashMap::new(),
            learnt: 0,
            covered: Coverage::default(),
            not_ours_seen: false,
            queries: HashMap::new(),
            ladder_notes: Vec::new(),
            tombstones: HashSet::new(),
        }
    }

    /// Where the Safety cancels of unprotected orders go: the session's `orders`, under the
    /// account `acct`. Attached before the session runs, so before it tells any. The link times
    /// its submissions on that session's shard clock from then on ([`Link::now`]).
    pub fn attach(&mut self, orders: ExecOrders, acct: AccountKey) {
        self.take_clock(&orders);
        self.session = Some((orders, acct));
    }

    /// Takes the shard clock of the session `orders` submits to, for [`Link::now`]: one other
    /// than the clock last taken is noted as a problem, and read from then on (decision 0155
    /// item 3).
    fn take_clock(&mut self, orders: &ExecOrders) {
        if self
            .clock
            .as_ref()
            .is_some_and(|last| !last.shares_clock(orders))
        {
            self.problem(
                "a session on another shard clock than the one the link timed its sends on: its \
                 send times are now on that session's clock, and those before on the other \
                 (decision 0155 item 3)"
                    .to_owned(),
            );
        }
        self.clock = Some(orders.clone());
    }

    /// The Safety cancel request submitted for our order `cid` when the session told it
    /// unprotected, if one was, until that request is released ([`Link::release`]) or dropped
    /// ([`Link::forget`], module documentation).
    pub fn safety_cancel_of(&self, cid: ClientOrderId) -> Option<RpcId> {
        self.safety.get(&cid).copied()
    }

    /// How many orders of ours on `inst` the session told unprotected so far.
    pub fn told_on(&self, inst: InstrumentId) -> usize {
        let mut cids: Vec<ClientOrderId> = self
            .notes
            .iter()
            .filter_map(|n| match n {
                Note::Unprotected { told, .. } => Some(told),
                _ => None,
            })
            .flatten()
            .filter(|(_, i)| *i == inst)
            .map(|(cid, _)| *cid)
            .collect();
        cids.sort_unstable();
        cids.dedup();
        cids.len()
    }

    /// The registry, for building commands and arming markets.
    pub fn reg(&mut self) -> &mut Registry {
        &mut self.reg
    }

    /// The registry, to read.
    pub fn registry(&self) -> &Registry {
        &self.reg
    }

    /// The venue's order caps.
    pub fn caps(&self) -> &OrderCaps {
        &self.caps
    }

    /// The fill ledger every fill goes through, to read its horizon, the fills it holds and its
    /// replay counts.
    pub fn ledger(&self) -> &FillLedger {
        &self.ledger
    }

    /// Each fill the ledger refused for its time, in order: every one it did not apply but a
    /// duplicate or one before its watermark, which the position already holds. Those
    /// [`Link::take_fill_refusals`] took are gone from it, so its length counts every refusal only
    /// for a consumer that never takes them; one that never does keeps every refusal for the
    /// link's life.
    pub fn fill_refusals(&self) -> &[FillRefusal] {
        &self.fill_refusals
    }

    /// Takes every fill refusal [`Link::fill_refusals`] holds out and hands them over, in order,
    /// for a consumer that reads each refusal once and runs for long: the list then holds only
    /// the refusals since its last take and does not grow with the run's length (FBC-2dr2,
    /// decision 0155 items 2 and 8; no bound, so no configuration). No refusal is lost before it
    /// is read: one the ledger makes after this call is held for the next. [`Link::ledger`]'s
    /// replay counts still count every refusal.
    pub fn take_fill_refusals(&mut self) -> Vec<FillRefusal> {
        std::mem::take(&mut self.fill_refusals)
    }

    /// Everything noted so far, in order.
    pub fn notes(&self) -> &[Note] {
        &self.notes
    }

    /// What the link sent for the Unknown ladder and the queries of unprotected orders, and what
    /// answered it, in order (module documentation): every [`LadderNote`] since
    /// [`Link::take_ladder_notes`] last took them, so a consumer that never takes them keeps them
    /// all.
    pub fn ladder_notes(&self) -> &[LadderNote] {
        &self.ladder_notes
    }

    /// Takes every [`LadderNote`] out and hands them over, in order, for a consumer that reads
    /// each once and runs for long: one the link notes after this call is held for the next.
    pub fn take_ladder_notes(&mut self) -> Vec<LadderNote> {
        std::mem::take(&mut self.ladder_notes)
    }

    /// How many places (requests whose items are placements) and cancels (cancels,
    /// cancel-manys, cancel-alls) submitted here the session reported sent, those in notes
    /// [`Link::forget`] took included; an amend is neither.
    pub fn sent_counts(&self) -> (usize, usize) {
        let (places, cancels) = self.counted(&self.notes);
        (self.forgotten.0 + places, self.forgotten.1 + cancels)
    }

    /// Forgets request `rpc`'s items at once, for a consumer that has every answer to it it
    /// waits for: the map of submitted requests then holds those in flight, not the run's
    /// history. [`Link::forget`] drops a request answered in full without it (module
    /// documentation), so a consumer that forgets its notes need not release; one that never
    /// forgets them releases each request itself. An answer to it that comes later is noted as
    /// the session's own ([`Note::Outcome`] with `ours` false), [`Link::is_cancel_on`] is false
    /// for it, and [`Link::outcome_of`], [`Link::cids_of`] and [`Link::safety_cancel_of`] no
    /// longer name it. Releasing a cancel the coverage of cancels not ours waits on an answer to
    /// ([`Coverage::waits_on`]) leaves an order end held on it unjudged: that answer reaches no
    /// coverage, so a refusal notes no [`Note::CancelNotOurs`]. A query released
    /// ([`LadderNote::Query`]) is forgotten: its answer reaches neither the registry nor a
    /// [`LadderNote::QueryAnswer`], and fbc-oms's ladder gives a ladder query up for resyncs
    /// once its intent timeout passes.
    pub fn release(&mut self, rpc: RpcId) {
        self.sent.remove(&rpc);
        self.cancels.remove(&rpc);
        self.answered.remove(&rpc);
        self.safety.retain(|_, r| *r != rpc);
        self.queries.remove(&rpc);
        self.tombstones.remove(&rpc);
    }

    /// How many submitted requests the link still holds the items of: those neither released
    /// ([`Link::release`]) nor dropped by [`Link::forget`] once answered in full and not waited on
    /// by the coverage, at the forget after the one that took their last note (module
    /// documentation).
    pub fn requests_held(&self) -> usize {
        self.sent.len()
    }

    /// Request `rpc`, submitted here, was answered `outcome` for item `item` (`None`: the whole
    /// request). An index past the request's items answers none of them. A request naming no
    /// order (a cancel-all) has one slot, which any answer answers.
    fn answer(&mut self, rpc: RpcId, item: Option<u16>, outcome: &SubmitOutcome) {
        let items = self.sent.get(&rpc).map_or(0, Vec::len);
        let slots = items.max(1);
        let level = match outcome {
            SubmitOutcome::Accepted {
                ack: AckLevel::Provisional,
            }
            | SubmitOutcome::Unknown => Answered::Open,
            _ => Answered::Final,
        };
        let range = match item.map(usize::from) {
            Some(idx) if items > 0 => idx..idx + 1,
            _ => 0..slots,
        };
        let answered = self
            .answered
            .entry(rpc)
            .or_insert_with(|| vec![Answered::Not; slots]);
        for slot in answered.get_mut(range).into_iter().flatten() {
            *slot = (*slot).max(level);
        }
    }

    /// Drops each request answered in full that no note left names: an earlier
    /// [`Link::forget`] took its last note, whose consumer has read it (module documentation).
    /// An item is answered in full by a final answer, to it or to the whole request, or by any
    /// answer once its order has ended in the registry; the one slot of a request naming no
    /// order (a cancel-all), whose answers fbc-oms reads none of, by any answer. A request the
    /// coverage still waits on an answer to ([`Coverage::waits_on`]) is kept, so that answer
    /// still routes as ours and reaches [`Coverage::accepted`] or [`Coverage::refused`]. Once one
    /// is dropped the coverage never waits on it again: reported already, it covers no order
    /// anew, and an order it covers, accepted for it or ended, is never held on it. A note
    /// naming a request is the report that it was or was not sent, or an answer to it; the
    /// [`Note::Unprotected`] naming a Safety cancel comes before both.
    fn drop_answered(&mut self) {
        let named: HashSet<RpcId> = self
            .notes
            .iter()
            .filter_map(|note| match note {
                Note::Submitted { rpc, .. } | Note::Outcome { rpc, .. } => Some(*rpc),
                _ => None,
            })
            .collect();
        let (sent, reg, covered) = (&self.sent, &self.reg, &self.covered);
        let live = |cid: &ClientOrderId| reg.get(*cid).is_some_and(|r| !r.state().is_terminal());
        // Whether the order an item names has ended, or the slot names none.
        let ended = |rpc: RpcId, item: Option<&Item>| {
            item.is_none_or(|it| {
                let cid = Link::op(*it, rpc).0;
                reg.get(cid).is_none_or(|r| r.state().is_terminal())
            })
        };
        let done: Vec<RpcId> = self
            .answered
            .iter()
            .filter(|(rpc, slots)| {
                let items = sent.get(rpc).map_or(&[][..], Vec::as_slice);
                !named.contains(rpc)
                    && !covered.waits_on(**rpc, live)
                    && slots.iter().enumerate().all(|(idx, slot)| match slot {
                        Answered::Final => true,
                        Answered::Open => ended(**rpc, items.get(idx)),
                        Answered::Not => false,
                    })
            })
            .map(|(rpc, _)| *rpc)
            .collect();
        for rpc in done {
            self.release(rpc);
        }
        debug_assert!(
            self.answered.keys().all(|rpc| self.sent.contains_key(rpc)),
            "the link records answers only of requests it holds"
        );
    }

    /// Whether request `rpc`, submitted through [`Link::submit`] and not yet released
    /// ([`Link::release`]) or dropped ([`Link::forget`]), is a cancel on market `inst`: an
    /// explicit cancel (a cancel or a cancel-many) naming at least one order of ours on `inst`,
    /// whether that order ended since or not, each order's market as fbc-oms built the cancel
    /// from its record, or a cancel-all of `inst`, a Safety cancel of
    /// [`ExecHandler::on_unprotected`] included. A place, a batch place or an amend is not, nor
    /// a cancel naming no order on `inst`. False for a request the link never submitted, has
    /// released, or has dropped: a consumer hands each note to a sweep running
    /// ([`crate::sweep::Sweep::observe`]) before it releases the note's request, and before its
    /// next [`Link::forget`], which drops a request answered in full whose last note the forget
    /// before took (module documentation).
    pub fn is_cancel_on(&self, rpc: RpcId, inst: InstrumentId) -> bool {
        self.cancels.get(&rpc).is_some_and(|c| c.on(inst))
    }

    /// What [`Link::sent_counts`] counts in `notes`.
    fn counted(&self, notes: &[Note]) -> (usize, usize) {
        let mut counts = (0, 0);
        for note in notes {
            let Note::Submitted { rpc, sent: Ok(()) } = note else {
                continue;
            };
            match self.sent.get(rpc).and_then(|items| items.first()) {
                Some(Item::Place(_)) => counts.0 += 1,
                Some(Item::Cancel(_)) | None if self.sent.contains_key(rpc) => counts.1 += 1,
                Some(Item::Amend { .. }) => {}
                _ => {}
            }
        }
        counts
    }

    /// Whether the link ever noted a cancel not ours ([`Note::CancelNotOurs`], module
    /// documentation): true from the first one on, never false again, and kept when
    /// [`Link::forget`] takes that note. Another program may then be on the account, and an
    /// instrument cancel-all would cancel its orders too (decision 0155 item 2).
    pub fn cancels_not_ours_seen(&self) -> bool {
        self.not_ours_seen
    }

    /// The orders of ours on `inst` the link holds live (decision 0155 item 2): placed through
    /// [`Link::submit`], shown open by a resync or told unprotected, and not ended in the
    /// registry, in the order the link learnt them (a resync's in the order it showed them), so
    /// a sweep's explicit cancels name them in the order the consumer met them. A command the
    /// consumer handed the session another way is not among them. What a sweep cancels and
    /// waits on ([`crate::sweep`]).
    pub fn live_on(&self, inst: InstrumentId) -> Vec<ClientOrderId> {
        let reg = &self.reg;
        let live = |cid: &ClientOrderId| reg.get(*cid).is_some_and(|r| !r.state().is_terminal());
        let mut found: Vec<(u64, ClientOrderId)> = self
            .live
            .iter()
            .filter(|(cid, (market, _))| *market == inst && live(cid))
            .map(|(cid, (_, learnt))| (*learnt, *cid))
            .collect();
        found.sort_unstable();
        found.into_iter().map(|(_, cid)| cid).collect()
    }

    /// Our order `cid` on `inst` is live: learnt now, unless the link already knows it.
    fn learn(&mut self, cid: ClientOrderId, inst: InstrumentId) {
        if let Entry::Vacant(slot) = self.live.entry(cid) {
            slot.insert((inst, self.learnt));
            self.learnt += 1;
        }
    }

    /// Takes the first `upto` notes out and hands them over, for a consumer that reads each
    /// note once and runs for long: the list then never grows with the run. What
    /// [`Link::sent_counts`] counts of them is kept, as is [`Link::cancels_not_ours_seen`];
    /// [`Link::outcome_of`], [`Link::login_refusals`] and [`Link::told_on`] see only the notes
    /// left. Before it takes any, it drops each request answered in full that no note left
    /// names, as [`Link::release`] does (module documentation): every item answered for good, or
    /// answered once its order has ended, and no answer to it waited on by the coverage of
    /// cancels not ours ([`Coverage::waits_on`]), a wait with no bound: a cancel whose held order
    /// end is never answered is kept for the link's life, unless the consumer releases it. A
    /// request whose last note this call takes is held
    /// until the next, so the consumer reads the notes it hands over, with
    /// [`Link::is_cancel_on`] and [`Link::cids_of`], against the link as it was. A consumer that
    /// never calls it keeps every note and every request they name, as before.
    pub fn forget(&mut self, upto: usize) -> Vec<Note> {
        self.drop_answered();
        let upto = upto.min(self.notes.len());
        let taken: Vec<Note> = self.notes.drain(..upto).collect();
        self.prune_ended();
        let (places, cancels) = self.counted(&taken);
        self.forgotten.0 += places;
        self.forgotten.1 += cancels;
        taken
    }

    /// What became of request `rpc`, submitted here, once every item is answered: not sent;
    /// the whole request's outcome; or, item by item, `Accepted` (at the first item's level)
    /// when every item was accepted and otherwise the first item's outcome that was not. `None`
    /// while an item is unanswered, so a batch is never judged by its first item alone, and once
    /// the request is released ([`Link::release`]) or dropped ([`Link::forget`], module
    /// documentation).
    pub fn outcome_of(&self, rpc: RpcId) -> Option<SubmitOutcome> {
        let items = self.sent.get(&rpc)?.len().max(1);
        let mut by_item: HashMap<u16, &SubmitOutcome> = HashMap::new();
        for note in &self.notes {
            match note {
                Note::Submitted {
                    rpc: r,
                    sent: Err(reason),
                } if *r == rpc => return Some(SubmitOutcome::NotSent(*reason)),
                Note::Outcome {
                    rpc: r,
                    item,
                    outcome,
                    ours: true,
                } if *r == rpc => match item {
                    None => return Some(outcome.clone()),
                    Some(idx) => {
                        by_item.entry(*idx).or_insert(outcome);
                    }
                },
                _ => {}
            }
        }
        if by_item.len() < items {
            return None;
        }
        let mut answered: Vec<(&u16, &&SubmitOutcome)> = by_item.iter().collect();
        answered.sort_by_key(|(idx, _)| **idx);
        let refused = answered
            .iter()
            .find(|(_, o)| !matches!(o, SubmitOutcome::Accepted { .. }));
        Some(match refused {
            Some((_, o)) => (**o).clone(),
            None => (*answered[0].1).clone(),
        })
    }

    /// The orders request `rpc`, submitted here, names, until it is released ([`Link::release`])
    /// or dropped ([`Link::forget`], module documentation): none after.
    pub fn cids_of(&self, rpc: RpcId) -> Vec<ClientOrderId> {
        let items = self.sent.get(&rpc).map_or(&[][..], Vec::as_slice);
        items.iter().map(|it| Link::op(*it, rpc).0).collect()
    }

    /// How many logins were refused in a row since a connection last authenticated, and the
    /// last refusal: `None` when none was.
    pub fn login_refusals(&self) -> Option<(usize, &str)> {
        let since = self
            .notes
            .iter()
            .rposition(|n| matches!(n, Note::Conn(ConnState::Authenticated)))
            .map_or(0, |at| at + 1);
        let refused: Vec<&str> = self.notes[since..]
            .iter()
            .filter_map(|n| match n {
                Note::LoginRefused(why) => Some(why.as_str()),
                _ => None,
            })
            .collect();
        refused.last().map(|last| (refused.len(), *last))
    }

    /// Now on the shard clock of the session the link was last attached to or submitted
    /// through, on its monotonic clock from the clock's origin and on the wall clock
    /// ([`ExecOrders::now`], decision 0155 item 3): `None` before the first attach or submit.
    /// The link times each request it submits by it; a consumer may read it for what it times
    /// on the session's clock, such as a planner's pass.
    pub fn now(&self) -> Option<(MonoNs, WallNs)> {
        self.clock.as_ref().map(ExecOrders::now)
    }

    /// One retention pass of the registry at `now`, the consumer's time on the shard's
    /// monotonic clock, under the consumer's `cfg` and the link's order caps
    /// ([`Registry::retain_ended`], decision 0157): what it forgot, kept and pinned. The
    /// consumer calls it after [`Link::forget`] and its reading of the notes that forget handed
    /// over, on a journaled timer, its resync bound sized as the module documentation says. The
    /// link reads no clock and keeps no timer.
    pub fn retain_ended(&mut self, cfg: &RetentionConfig, now: MonoNs) -> Retained {
        self.reg.retain_ended(cfg, &self.caps, now)
    }

    /// Submits `auth` through `orders`, remembering the order each item names: its request id,
    /// or why it was not submitted (nothing was then remembered). A cancel covers the orders it
    /// names, and a cancel-all every live order of ours on its market now, once the session
    /// reports it sent (module documentation): until then it is pending. The link times its
    /// sends on `orders`' shard clock from then on ([`Link::now`]).
    pub fn submit(&mut self, orders: &ExecOrders, auth: Authorization) -> Result<RpcId, String> {
        let items = self.items_of(auth.command())?;
        let scope = match auth.command() {
            VenueCommand::CancelAll(scope) => Some(*scope),
            _ => None,
        };
        // Each market a cancel cancels on, from the command fbc-oms authorized.
        let cancels = match auth.command() {
            VenueCommand::Cancel(c) => Some(Cancels::Orders(vec![c.inst])),
            VenueCommand::CancelMany(cs) => {
                Some(Cancels::Orders(cs.iter().map(|c| c.inst).collect()))
            }
            _ => scope.map(Cancels::All),
        };
        let placed: Vec<(ClientOrderId, InstrumentId)> = match auth.command() {
            VenueCommand::Place(o) => vec![(o.cid, o.inst)],
            VenueCommand::PlaceBatch(os) => os.iter().map(|o| (o.cid, o.inst)).collect(),
            _ => Vec::new(),
        };
        self.take_clock(orders);
        let rpc = orders.submit(auth).map_err(|e| format!("{e:?}"))?;
        let named = items.iter().filter_map(|item| match item {
            Item::Cancel(cid) => Some(*cid),
            _ => None,
        });
        self.covered.submitted(rpc, named);
        if let Some(scope) = scope {
            self.prune_ended();
            let all: Vec<ClientOrderId> = self
                .live
                .iter()
                .filter(|(_, (inst, _))| match scope {
                    CancelScope::Instrument(market) => *inst == market,
                    CancelScope::Account => true,
                })
                .map(|(cid, _)| *cid)
                .collect();
            self.covered.submitted(rpc, all);
        }
        for (cid, inst) in placed {
            self.learn(cid, inst);
        }
        self.sent.insert(rpc, items);
        if let Some(cancels) = cancels {
            self.cancels.insert(rpc, cancels);
        }
        Ok(rpc)
    }

    /// One pass of fbc-oms's Unknown ladder at `now`, the consumer's timer's time on the
    /// session's shard clock ([`Link::now`], or a consumer timer's `at`), under the
    /// [`LadderConfig`] given to [`Link::new`] (module documentation). Each query it plans is
    /// submitted through `orders` as a control command and recorded with
    /// [`Registry::query_sent`]; each tombstone cancel is authorized for `orders`' account and
    /// submitted through [`Link::submit`], and recorded with [`Registry::tombstone_sent`] once
    /// the session reports it sent. A query the session refuses was not recorded, so fbc-oms's
    /// ladder gives it up for resyncs once its intent timeout passes; a tombstone not sent is
    /// built again a maximum time later. Before it sends anything, it forgets each query
    /// acknowledged whose answer is no longer awaited (module documentation). The link reads no
    /// clock and keeps no timer: the consumer calls it on its own.
    pub fn ladder(&mut self, orders: &ExecOrders, now: MonoNs) -> LadderPass {
        let plan = self.reg.ladder(&self.ladder, &self.caps, now);
        self.forget_unawaited(now);
        let mut pass = LadderPass {
            escalated: plan.escalated,
            resync_wanted: plan.resync,
            ..LadderPass::default()
        };
        for (cid, cmd) in plan.queries {
            self.take_clock(orders);
            match orders.submit_control(cmd) {
                Ok(rpc) => {
                    // The ladder plans a query only for an order the registry holds.
                    let refused = self.reg.query_sent(cid, rpc).err();
                    let problem = |e| Note::Problem(format!("request {}: {e:?}", rpc.0));
                    self.notes.extend(refused.map(problem));
                    self.queried(cid, rpc, Queried::Ladder, Some(now));
                    pass.queries.push((cid, rpc));
                }
                Err(e) => pass
                    .unsent
                    .push((cid, LadderCommand::Query, format!("{e:?}"))),
            }
        }
        let acct = orders.account();
        for (cid, cmd) in plan.tombstones {
            let submitted = self
                .reg
                .authorize(acct, cmd)
                .map_err(|e| format!("{e:?}"))
                .and_then(|auth| self.submit(orders, auth));
            match submitted {
                Ok(rpc) => {
                    self.tombstones.insert(rpc);
                    self.ladder_notes.push(LadderNote::Tombstone { cid, rpc });
                    pass.tombstones.push((cid, rpc));
                }
                Err(e) => pass.unsent.push((cid, LadderCommand::Tombstone, e)),
            }
        }
        for cid in plan.no_tombstone {
            let why = "the venue's single cancel cannot name a client id".to_owned();
            pass.unsent.push((cid, LadderCommand::Tombstone, why));
        }
        self.ladder_notes.extend(
            pass.unsent
                .iter()
                .map(|(cid, what, why)| LadderNote::Unsent {
                    cid: *cid,
                    what: *what,
                    why: why.clone(),
                }),
        );
        pass
    }

    /// The query of `cid` for `of` was submitted under `rpc` at `sent`: kept until answered,
    /// and noted.
    fn queried(&mut self, cid: ClientOrderId, rpc: RpcId, of: Queried, sent: Option<MonoNs>) {
        let query = Query {
            cid,
            of,
            sent,
            accepted: false,
        };
        self.queries.insert(rpc, query);
        self.ladder_notes.push(LadderNote::Query { cid, rpc, of });
    }

    /// Forgets at `now` each query acknowledged whose answer is no longer awaited, since the
    /// answer may never come: a ladder query once fbc-oms's ladder no longer waits on its order's
    /// query (it gave the query up for resyncs, or the order left the ladder), an unprotected
    /// order's once the intent timeout has passed since it was sent, as fbc-oms gives a ladder
    /// query up. One not acknowledged stays until its outcome, which its deadline brings.
    fn forget_unawaited(&mut self, now: MonoNs) {
        let reg = &self.reg;
        let timeout = self.ladder.intent_timeout();
        self.queries.retain(|_, q| {
            !q.accepted
                || match q.of {
                    Queried::Ladder => reg
                        .get(q.cid)
                        .is_some_and(|r| r.ladder_step() == Some(LadderStep::Querying)),
                    Queried::Unprotected => q.sent.is_some_and(|sent| now < sent + timeout),
                }
        });
    }

    /// The answer to a query held here: a ladder query's applied to the registry under `key`
    /// ([`Registry::on_query_answer`]), and noted.
    fn on_query_result(&mut self, answer: &QueryAnswer, key: OrderKey) {
        let rpc = answer.rpc();
        let Some(Query { cid, of, .. }) = self.queries.remove(&rpc) else {
            return;
        };
        let resolution = match of {
            Queried::Ladder => Some(self.reg.on_query_answer(&self.caps, answer, key)),
            Queried::Unprotected => None,
        };
        let read = match answer.found() {
            Some(found) => QueryRead::Shown(found.state.clone()),
            None => QueryRead::NotFound,
        };
        self.ladder_notes.push(LadderNote::QueryAnswer {
            cid,
            rpc,
            of,
            read,
            resolution,
        });
    }

    /// The outcome of query `rpc` held here, or the session's report that it was not sent: a
    /// ladder query's applied ([`Registry::on_query_outcome`]), and noted. Any outcome but an
    /// acceptance answers it; an acceptance leaves its answer to come, until
    /// [`Link::ladder`] forgets it.
    fn on_query_outcome(&mut self, rpc: RpcId, outcome: SubmitOutcome) {
        let Some(query) = self.queries.get_mut(&rpc) else {
            return;
        };
        let Query { cid, of, .. } = *query;
        if matches!(outcome, SubmitOutcome::Accepted { .. }) {
            query.accepted = true;
        } else {
            self.queries.remove(&rpc);
        }
        let resolution = match of {
            Queried::Ladder => Some(self.reg.on_query_outcome(rpc, &outcome)),
            Queried::Unprotected => None,
        };
        self.ladder_notes.push(LadderNote::QueryAnswer {
            cid,
            rpc,
            of,
            read: QueryRead::Outcome(outcome),
            resolution,
        });
    }

    /// Forgets the orders of ours the registry holds ended (or not at all) from what a cancel
    /// of ours covers and from the live orders: only the event that ends an order is judged, so
    /// neither keeps the run's history.
    fn prune_ended(&mut self) {
        let reg = &self.reg;
        let live = |cid: &ClientOrderId| reg.get(*cid).is_some_and(|r| !r.state().is_terminal());
        self.live.retain(|cid, _| live(cid));
        self.covered.retain(live);
    }

    /// Whether a cancel of ours covers our order `cid` (module documentation): only one
    /// reported sent and not refused. The registry's lifetime count of cancels sent is not read:
    /// it stays past a refusal (Codex r4309154204 on PR #159).
    fn cancel_of_ours(&self, cid: ClientOrderId) -> bool {
        self.covered.covers(cid)
    }

    /// The items of `cmd`, each naming one of our orders.
    fn items_of(&self, cmd: &VenueCommand) -> Result<Vec<Item>, String> {
        let cancel_of = |c: &fbc_core::CancelOrder| {
            c.target
                .client()
                .or_else(|| c.target.venue().and_then(|v| self.reg.cid_of(v)))
                .map(Item::Cancel)
                .ok_or_else(|| "a cancel of no order the registry holds".to_owned())
        };
        match cmd {
            VenueCommand::Place(o) => Ok(vec![Item::Place(o.cid)]),
            VenueCommand::PlaceBatch(orders) => {
                Ok(orders.iter().map(|o| Item::Place(o.cid)).collect())
            }
            VenueCommand::Cancel(c) => Ok(vec![cancel_of(c)?]),
            VenueCommand::CancelMany(cs) => cs.iter().map(cancel_of).collect(),
            VenueCommand::CancelAll(_) => Ok(Vec::new()),
            VenueCommand::Amend(a) => {
                let cid = a
                    .target
                    .client()
                    .or_else(|| a.target.venue().and_then(|v| self.reg.cid_of(v)))
                    .ok_or_else(|| "an amend of no order the registry holds".to_owned())?;
                Ok(vec![Item::Amend {
                    cid,
                    px: a.px,
                    qty: a.qty,
                }])
            }
            other => Err(format!("not modelled by this glue: {}", kind(other))),
        }
    }

    /// The order op an item of request `rpc` is.
    fn op(item: Item, rpc: RpcId) -> (ClientOrderId, OrderOp) {
        match item {
            Item::Place(cid) => (cid, OrderOp::Place),
            Item::Cancel(cid) => (cid, OrderOp::Cancel(rpc)),
            Item::Amend { cid, .. } => (cid, OrderOp::Amend(rpc)),
        }
    }

    fn problem(&mut self, what: String) {
        self.notes.push(Note::Problem(what));
    }

    /// What became of a submission, as [`ExecHandler::on_submitted`] reports it (module
    /// documentation): sent, the send is recorded; not sent, every item's order is told so.
    pub fn on_submitted(&mut self, handle: SubmitHandle) {
        let SubmitHandle { rpc, receipt } = handle;
        let items = self.sent.get(&rpc).cloned().unwrap_or_default();
        // Only a request submitted here holds items, and submitting took a clock: before any,
        // no item is timed, so the clock's origin stands for none.
        let (mono, wall) = self.now().unwrap_or((MonoNs(0), WallNs(0)));
        for (idx, item) in items.into_iter().enumerate() {
            let (cid, op) = Link::op(item, rpc);
            // An amend the session took is in flight until its outcome resolves it, a not-sent
            // one included (fbc-oms's `amend_not_submitted` is for one never handed over).
            if let Item::Amend { px, qty, .. } = item
                && let Err(e) = self.reg.amend_sent(cid, px, qty, rpc, mono)
            {
                self.problem(format!("request {}: {e:?}", rpc.0));
            }
            let done = match &receipt {
                Ok(_) => match item {
                    Item::Place(_) => self.reg.placement_sent(cid, mono, wall),
                    // The ladder's tombstone, recorded as one so its outcome resolves the order.
                    Item::Cancel(_) if self.tombstones.contains(&rpc) => {
                        self.reg.tombstone_sent(cid, rpc, mono).map(|_| ())
                    }
                    Item::Cancel(_) => self.reg.cancel_sent(cid, rpc, mono).map(|_| ()),
                    Item::Amend { .. } => Ok(()),
                },
                Err(reason) => {
                    let item = ItemRef {
                        idx: u16::try_from(idx).unwrap_or(u16::MAX),
                        cid: Some(cid),
                        vid: None,
                    };
                    let outcome = SubmitOutcome::NotSent(*reason);
                    self.reg
                        .on_outcome(cid, op, &item, &outcome, mono)
                        .map(|_| ())
                }
            };
            if let Err(e) = done {
                self.problem(format!("request {}: {e:?}", rpc.0));
            }
            if let (Ok(used), Item::Place(_)) = (&receipt, item) {
                self.placement_nonce(rpc, idx, cid, used);
            }
        }
        let sent = receipt.map(|_| ());
        if let Err(reason) = sent
            && self.sent.contains_key(&rpc)
        {
            self.answer(rpc, None, &SubmitOutcome::NotSent(reason));
        }
        self.covered.reported(rpc, sent.is_ok());
        let unsent = sent.err();
        self.notes.push(Note::Submitted { rpc, sent });
        // A query not sent is answered so: the ladder's then leaves its order to resyncs.
        if let Some(reason) = unsent {
            self.on_query_outcome(rpc, SubmitOutcome::NotSent(reason));
        }
    }

    /// Records the nonce `used` names for item `idx` of request `rpc`, a place of `cid`, as its
    /// order's placement nonce: nothing when it names none for the item; a refusal (another
    /// nonce already recorded, or no such order) noted as a problem by its kind and order, the
    /// nonce never written (0009).
    fn placement_nonce(
        &mut self,
        rpc: RpcId,
        idx: usize,
        cid: ClientOrderId,
        used: &EncodeReceipt,
    ) {
        let named = used
            .nonces()
            .iter()
            .find(|(item, _)| usize::from(*item) == idx);
        let Some(&(_, nonce)) = named else {
            return;
        };
        if let Err(e) = self.reg.placement_nonce_used(cid, nonce) {
            self.problem(format!(
                "request {}: placement nonce not recorded: {e:?}",
                rpc.0
            ));
        }
    }

    fn on_outcome(
        &mut self,
        rpc: RpcId,
        item: Option<ItemRef>,
        outcome: SubmitOutcome,
        now: MonoNs,
    ) {
        if self.queries.contains_key(&rpc) {
            self.on_query_outcome(rpc, outcome);
            return;
        }
        let Some(items) = self.sent.get(&rpc).cloned() else {
            self.notes.push(Note::Outcome {
                rpc,
                item: item.map(|i| i.idx),
                outcome,
                ours: false,
            });
            return;
        };
        // One item's outcome, or the whole request's (`item: None`).
        let targets: Vec<(Item, ItemRef)> = match &item {
            Some(named) => match items.get(usize::from(named.idx)) {
                Some(it) => vec![(*it, named.clone())],
                None => Vec::new(),
            },
            None => items
                .iter()
                .enumerate()
                .map(|(idx, it)| {
                    let cid = Link::op(*it, rpc).0;
                    let named = ItemRef {
                        idx: u16::try_from(idx).unwrap_or(u16::MAX),
                        cid: Some(cid),
                        vid: None,
                    };
                    (*it, named)
                })
                .collect(),
        };
        // A refused cancel of ours, or one not sent after all, stops covering what it names (a
        // cancel-all's item: the order it states, or every order it covers when it states none;
        // the whole request: every order it covers). An accepted one makes an order it covers
        // that a user cancel ended, held, ours.
        let scope = match &item {
            None => Some(None),
            Some(named) => match items.get(usize::from(named.idx)) {
                Some(Item::Cancel(cid)) => Some(Some(*cid)),
                Some(_) => None,
                None if items.is_empty() => Some(named.cid),
                None => None,
            },
        };
        let mut released = Vec::new();
        if let Some(cid) = scope {
            match &outcome {
                SubmitOutcome::Rejected(_) | SubmitOutcome::NotSent(_) => {
                    released = self.covered.refused(rpc, cid);
                }
                SubmitOutcome::Accepted { .. } => self.covered.accepted(rpc, cid),
                _ => {}
            }
        }
        let mut resolved = Vec::new();
        for (it, named) in targets {
            let (cid, op) = Link::op(it, rpc);
            match self.reg.on_outcome(cid, op, &named, &outcome, now) {
                Ok(OutcomeApplied::TombstoneResolved) => {
                    let state = self.reg.get(cid).map(|r| r.state());
                    let note = |state| LadderNote::TombstoneResolved { cid, rpc, state };
                    resolved.extend(state.map(note));
                }
                Ok(_) => {}
                Err(e) => self.problem(format!("request {}: {e:?}", rpc.0)),
            }
        }
        let item = item.map(|i| i.idx);
        self.answer(rpc, item, &outcome);
        self.notes.push(Note::Outcome {
            rpc,
            item,
            outcome,
            ours: true,
        });
        self.ladder_notes.extend(resolved);
        // Orders a user cancel ended while only this refused request covered them: a cancel
        // not ours, its order event read before the refusal (Codex r4310042612 on PR #159).
        self.not_ours_seen |= !released.is_empty();
        self.notes
            .extend(released.into_iter().map(|r| Note::CancelNotOurs {
                cid: r.cid,
                side: r.side,
                at: r.at,
            }));
    }

    fn on_resync_end(&mut self, ingest: u64, venue: Option<u64>) {
        let Some(snapshot) = self.collecting.take() else {
            self.problem("ResyncEnd with no ResyncBegin".to_owned());
            return;
        };
        let key = OrderKey { venue, ingest };
        for o in &snapshot.orders {
            if let Some(CidMatch::Ours(cid)) = o.cid {
                self.learn(cid, o.inst);
            }
        }
        match self.reg.resync(&self.ladder, &self.caps, &snapshot, key) {
            Ok(report) => self.notes.push(Note::Resynced { report, snapshot }),
            Err(e) => self.notes.push(Note::ResyncRefused(e.to_string())),
        }
    }

    /// An event the session handed its handler ([`ExecHandler::on_exec`]), routed into the
    /// registry and noted (module documentation).
    pub fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let now = env.stamp.recv_mono;
        self.last_recv = Some(now);
        let key = OrderKey {
            venue: env.venue_seq,
            ingest: env.stamp.ingest_seq,
        };
        match env.body {
            ExecEvent::Outcome { rpc, item, outcome } => self.on_outcome(rpc, item, outcome, now),
            ExecEvent::Order(u) => {
                // Our order this event names, and whether it had not ended before it.
                let named = match u.cid {
                    Some(CidMatch::Ours(cid)) => Some(cid),
                    _ => u.vid.as_ref().and_then(|v| self.reg.cid_of(v)),
                };
                let was_live = named
                    .and_then(|cid| self.reg.get(cid))
                    .is_some_and(|r| !r.state().is_terminal());
                let routed = self.reg.apply_update(&u, key);
                let not_ours = match routed {
                    Routed::Ours(cid, Applied::Advanced)
                        if was_live
                            && named == Some(cid)
                            && u.state
                                == fbc_core::VenueOrderState::Canceled(CancelReason::Requested) =>
                    {
                        let side = self
                            .reg
                            .get(cid)
                            .filter(|r| {
                                r.state()
                                    == OrdState::Terminal(TerminalKind::Canceled(
                                        CancelReason::Requested,
                                    ))
                            })
                            .map(|r| r.placed().side);
                        match side {
                            Some(side) if !self.cancel_of_ours(cid) => Some(Note::CancelNotOurs {
                                cid,
                                side,
                                at: env.exch_ts,
                            }),
                            // Covered only by requests not yet answered: held until they are,
                            // noted if all are refused (Codex r4310042612 on PR #159).
                            Some(side) => {
                                self.covered.hold(cid, side, env.exch_ts);
                                None
                            }
                            None => None,
                        }
                    }
                    _ => None,
                };
                let note = match (routed, u.cid) {
                    (Routed::Untracked, Some(CidMatch::Ours(cid))) => {
                        Note::Orphan { cid, vid: u.vid }
                    }
                    (routed, _) => Note::Order(routed),
                };
                self.notes.push(note);
                self.not_ours_seen |= not_ours.is_some();
                self.notes.extend(not_ours);
            }
            ExecEvent::Fill(f) => {
                let time = env.exch_ts.map(|exch| FillTime {
                    exch,
                    kind: env.exch_ts_kind,
                    aligned: WallNs(exch.0),
                });
                let (seen, unexplained, ours) = match self.ledger.admit(&f, time, now) {
                    Admission::Apply(accepted) => match self.reg.apply_fill(accepted) {
                        Ok(routed) => (
                            describe_fill(&routed),
                            !matches!(routed, FillRouted::Ours(..) | FillRouted::InSnapshot(_)),
                            matches!(routed, FillRouted::Ours(..)),
                        ),
                        Err(e) => (format!("refused by the registry: {e:?}"), true, false),
                    },
                    held @ (Admission::Duplicate | Admission::BeforeWatermark) => {
                        (format!("not applied: {held:?}"), false, false)
                    }
                    refused @ (Admission::BeyondHorizon | Admission::Untimed) => {
                        let admission = match refused {
                            Admission::BeyondHorizon => "BeyondHorizon",
                            _ => "Untimed",
                        };
                        let at = now;
                        self.fill_refusals.push(FillRefusal { admission, at });
                        (format!("not applied: {admission}"), true, false)
                    }
                };
                self.notes.push(Note::Fill {
                    seen,
                    unexplained,
                    ours,
                });
            }
            ExecEvent::ResyncBegin { watermark } => {
                let requested_at = self.authenticated_at.unwrap_or(now);
                self.collecting = Some(ResyncSnapshot {
                    watermark,
                    requested_at,
                    orders: Vec::new(),
                    positions: Vec::new(),
                });
            }
            ExecEvent::ResyncOrder(o) => match self.collecting.as_mut() {
                Some(snap) => snap.orders.push(o),
                None => self.problem("ResyncOrder outside a resync".to_owned()),
            },
            ExecEvent::ResyncPosition { inst, qty, .. } => match self.collecting.as_mut() {
                Some(snap) => snap.positions.push((inst, qty)),
                None => self.problem("ResyncPosition outside a resync".to_owned()),
            },
            ExecEvent::ResyncEnd => self.on_resync_end(env.stamp.ingest_seq, env.venue_seq),
            ExecEvent::QueryResult(answer) => self.on_query_result(&answer, key),
            ExecEvent::Conn { state, .. } => {
                match state {
                    ConnState::Authenticated => {
                        self.authenticated_at = Some(now);
                        self.epoch_authenticated = true;
                    }
                    ConnState::Closed => self.epoch_closed = true,
                    _ => {}
                }
                self.notes.push(Note::Conn(state));
            }
            ExecEvent::Position { inst, qty, .. } => {
                self.notes.push(Note::Position { inst, qty });
            }
            ExecEvent::AsyncReject { target, op, reject } => {
                self.notes.push(Note::AsyncReject { target, op, reject });
            }
            ExecEvent::UncorrelatedError(reject) => {
                let why = described(&reject);
                self.epoch_error = Some(why.clone());
                if self.epoch_authenticated {
                    self.problem(format!(
                        "venue error naming no request: {:?}, {why}",
                        reject.kind
                    ));
                } else {
                    self.notes.push(Note::LoginRefused(why));
                }
            }
            // The account's balances, funding, modes and fee rates move nothing the registry
            // holds here.
            _ => {}
        }
    }

    /// The orders of ours the session told unprotected ([`ExecHandler::on_unprotected`], module
    /// documentation): each the registry holds open gets a Safety cancel at once, and each it
    /// holds ended or not at all a query through the attached session, where the venue's caps
    /// declare a reference for queries the query carries.
    pub fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        let told: Vec<(ClientOrderId, InstrumentId)> = orders
            .iter()
            .filter_map(|o| match o.cid {
                Some(CidMatch::Ours(cid)) => Some((cid, o.inst)),
                _ => None,
            })
            .collect();
        for (cid, inst) in &told {
            self.learn(*cid, *inst);
        }
        let held_open =
            |reg: &Registry, cid| reg.get(cid).is_some_and(|r| !r.state().is_terminal());
        let open: Vec<ClientOrderId> = told
            .iter()
            .map(|(cid, _)| *cid)
            .filter(|cid| held_open(&self.reg, *cid))
            .collect();
        // Each the registry holds ended or not at all, named by both our client id and the
        // venue id it was shown under, and its placement nonce where the registry holds one:
        // the codec chooses what its venue's queries declare.
        let queries: Vec<(ClientOrderId, QueryOrder)> = orders
            .iter()
            .filter_map(|o| match o.cid {
                Some(CidMatch::Ours(cid)) if !held_open(&self.reg, cid) => Some((cid, o)),
                _ => None,
            })
            .map(|(cid, o)| {
                let query = QueryOrder {
                    target: OrderRef::Both(cid, o.vid.clone()),
                    inst: o.inst,
                    placement_nonce: self.reg.get(cid).and_then(|r| r.placement_nonce()),
                };
                (cid, query)
            })
            .collect();
        // The Safety cancels go to the session first, so it encodes them before any query
        // (decision 0080: at once); the texts keep the queries' first.
        let plan = self.reg.cancel_many(&open, &self.caps);
        let unbuilt = plan.refused.iter().map(|(cid, r)| (*cid, format!("{r:?}")));
        let waiting = plan
            .awaiting_ack
            .iter()
            .map(|cid| (*cid, "awaits its ack".to_owned()));
        let mut cancels_left: Vec<String> = unbuilt
            .chain(waiting)
            .map(|(cid, why)| format!("client id sequence {}: no cancel built: {why}", cid.seq()))
            .collect();
        let mut rpcs = Vec::new();
        let session = self.session.clone();
        for cmd in plan.commands {
            let submitted = session
                .as_ref()
                .ok_or_else(|| "no session attached".to_owned())
                .and_then(|(orders, acct)| {
                    let auth = self
                        .reg
                        .authorize(*acct, cmd)
                        .map_err(|e| format!("{e:?}"))?;
                    self.submit(orders, auth)
                });
            match submitted {
                Ok(rpc) => {
                    for cid in self.cids_of(rpc) {
                        self.safety.insert(cid, rpc);
                    }
                    rpcs.push(rpc);
                }
                Err(e) => cancels_left.push(format!("a Safety cancel was not submitted: {e}")),
            }
        }
        let (mut left, mut queried) = (Vec::new(), Vec::new());
        for (cid, query) in queries {
            let what = if query.reference(self.caps.query_refs).is_none() {
                "the venue's caps declare no reference for queries that it carries, so no query is \
                 sent"
                    .to_owned()
            } else {
                let submitted = session
                    .as_ref()
                    .ok_or_else(|| "no session attached".to_owned())
                    .and_then(|(orders, _)| {
                        self.take_clock(orders);
                        let cmd = ControlCommand::Query(query);
                        orders.submit_control(cmd).map_err(|e| format!("{e:?}"))
                    });
                match submitted {
                    Ok(rpc) => {
                        queried.push((cid, rpc, self.now().map(|(mono, _)| mono)));
                        format!("it is queried (request {})", rpc.0)
                    }
                    Err(e) => format!("its query was not submitted: {e}"),
                }
            };
            left.push(format!(
                "client id sequence {}: the registry holds it ended or not at all, so no cancel \
                 is built; {what}; its market stays held until a query's answer shows it ended",
                cid.seq()
            ));
        }
        left.extend(cancels_left);
        self.notes.push(Note::Unprotected { told, rpcs, left });
        for (cid, rpc, sent) in queried {
            self.queried(cid, rpc, Queried::Unprotected, sent);
        }
    }

    /// Connection epoch `key` ended ([`ExecHandler::on_epoch_end`]): reported to the registry at
    /// the last envelope's receive time (module documentation), noted with why
    /// ([`Note::EpochEnd`]), and the next starts unauthenticated with nothing said.
    pub fn on_epoch_end(&mut self, key: ConnKey) {
        self.reg
            .order_entry_epoch_ended(self.last_recv.unwrap_or(MonoNs(0)));
        let why = match (self.epoch_error.take(), self.epoch_closed) {
            (Some(error), _) => error,
            (None, true) => CODEC_CLOSED.to_owned(),
            (None, false) => ENDED_UNSAID.to_owned(),
        };
        self.epoch_authenticated = false;
        self.epoch_closed = false;
        self.notes.push(Note::EpochEnd { key, why });
    }

    /// A frame or HTTP answer the codec could not decode ([`ExecHandler::on_decode_error`]): a
    /// problem, which ends the consumer's trading (it may have held a fill, a position or an
    /// order event never seen).
    pub fn on_decode_error(&mut self, key: ConnKey) {
        self.problem(format!(
            "connection {key:?}: a frame or HTTP answer the codec could not decode (it may have \
             held a fill, a position or an order event never seen)"
        ));
    }
}

/// A fill's routing, without its amounts.
fn describe_fill(routed: &FillRouted) -> String {
    match routed {
        FillRouted::Ours(..) => "ours: counted on the order and the inventory".to_owned(),
        other => format!("{other:?}"),
    }
}

/// A command's kind, without its contents.
fn kind(cmd: &VenueCommand) -> &'static str {
    match cmd {
        VenueCommand::Place(_) => "place",
        VenueCommand::PlaceBatch(_) => "batch",
        VenueCommand::Amend(_) => "amend",
        VenueCommand::Cancel(_) => "cancel",
        VenueCommand::CancelMany(_) => "cancel-many",
        VenueCommand::CancelAll(_) => "cancel-all",
        VenueCommand::ArmCancelOnDisconnect(_) => "cancel-on-disconnect",
        VenueCommand::RefreshDeadMan => "dead-man refresh",
        VenueCommand::Query(_) => "query",
        VenueCommand::FeeQuery => "fee query",
    }
}

/// The session's handler: hands each call to the [`Link`], then calls `wake`, the consumer's
/// closure, so whoever waits on the link looks again (by waking a tokio `Notify`'s waiters,
/// say). The link is not borrowed while `wake` runs.
pub struct LinkHandler {
    pub link: Rc<RefCell<Link>>,
    pub wake: Box<dyn FnMut()>,
}

impl ExecHandler for LinkHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.link.borrow_mut().on_exec(env);
        (self.wake)();
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.link.borrow_mut().on_epoch_end(key);
        (self.wake)();
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.link.borrow_mut().on_submitted(handle);
        (self.wake)();
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.link.borrow_mut().on_unprotected(orders);
        (self.wake)();
    }

    /// A frame or HTTP answer the codec could not decode: [`Link::on_decode_error`].
    fn on_decode_error(&mut self, key: ConnKey) {
        self.link.borrow_mut().on_decode_error(key);
        (self.wake)();
    }
}

/// What the link says of a command it does not model, which no fbc-oms authorization carries:
/// its kind alone, never its contents.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fbc_core::{
        AccountKey, AmendOrder, CancelOrder, CancelScope, Channel, CidMint, InstrumentId, Lots,
        Namespace, NamespaceLease, NewOrder, OrderKind, OrderRef, QueryOrder, Side, Ticks, Tif,
        VenueCommand, WallNs, dispatch_market_data,
    };
    use fbc_oms::{FillLedger, LadderConfig, LedgerConfig, Registry};

    use super::{Link, kind};

    /// A link over a fresh registry under the conformance toy's order caps.
    fn link() -> Link {
        let caps = fbc_conformance::toy::caps().exec.unwrap().order;
        let ledger = FillLedger::new(
            LedgerConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 1_000,
            },
            WallNs(0),
        )
        .unwrap();
        let ladder = LadderConfig::new(
            Duration::from_secs(5),
            Duration::from_secs(1),
            Duration::from_secs(60),
            2,
        )
        .unwrap();
        Link::new(Registry::new(), ledger, caps, ladder)
    }

    #[test]
    fn a_command_is_named_by_its_kind_and_one_not_modelled_is_refused_by_it() {
        let dir = std::env::temp_dir().join(format!("fbc-live-link-unit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), Namespace::new(1)).unwrap();
        let cid = CidMint::new(lease, 0, 0, WallNs(1_000_000_000))
            .mint()
            .unwrap();
        let toy = fbc_conformance::toy::caps();
        let vid = dispatch_market_data(&toy, |s| s.venue_order_id("v-1")).unwrap();
        let inst = InstrumentId::new(7);
        let new = NewOrder {
            cid,
            inst,
            side: Side::Buy,
            qty: Lots::new(1).unwrap(),
            kind: OrderKind::Limit { px: Ticks(100) },
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let cancel = CancelOrder {
            target: OrderRef::Venue(vid.clone()),
            inst,
            side: Side::Buy,
            placement_nonce: None,
        };
        let amend = AmendOrder {
            target: OrderRef::Venue(vid.clone()),
            inst,
            side: Side::Buy,
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
            px: Ticks(99),
            qty: Lots::new(1).unwrap(),
            cum_filled: Lots::ZERO,
        };
        let query = QueryOrder {
            target: OrderRef::Venue(vid),
            inst,
            placement_nonce: None,
        };
        let named: Vec<&str> = [
            VenueCommand::Place(new.clone()),
            VenueCommand::PlaceBatch(vec![new]),
            VenueCommand::Amend(amend),
            VenueCommand::Cancel(cancel.clone()),
            VenueCommand::CancelMany(vec![cancel]),
            VenueCommand::CancelAll(CancelScope::Instrument(inst)),
            VenueCommand::ArmCancelOnDisconnect(true),
            VenueCommand::RefreshDeadMan,
            VenueCommand::Query(query.clone()),
            VenueCommand::FeeQuery,
        ]
        .iter()
        .map(kind)
        .collect();
        assert_eq!(
            named,
            [
                "place",
                "batch",
                "amend",
                "cancel",
                "cancel-many",
                "cancel-all",
                "cancel-on-disconnect",
                "dead-man refresh",
                "query",
                "fee query",
            ]
        );

        // A command no authorization carries is refused by its kind alone.
        let link = link();
        for (cmd, what) in [
            (
                VenueCommand::ArmCancelOnDisconnect(false),
                "cancel-on-disconnect",
            ),
            (VenueCommand::RefreshDeadMan, "dead-man refresh"),
            (VenueCommand::Query(query), "query"),
            (VenueCommand::FeeQuery, "fee query"),
        ] {
            assert_eq!(
                link.items_of(&cmd).err(),
                Some(format!("not modelled by this glue: {what}"))
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
