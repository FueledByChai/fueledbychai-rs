//! FBC-hwn's done line (design §5.4, decision 0104): an order-entry session bounds the commands
//! waiting for its connection and the requests it has in flight, each by the consumer's
//! configured Normal bound plus a reserve only Safety traffic (a cancel, a reducing order) may
//! use. With the conformance toy and a peer that stopped reading, a place submitted past the
//! connection's Normal bound is `NotSent(Backpressure)`, nothing of it written and the refusal
//! journaled as its write result, while a cancel uses the reserve and goes out once the peer
//! reads again; and a place submitted past the configured in-flight bound is
//! `NotSent(RateBudget)`, nothing written, while a cancel still goes out.
//!
//! Every order command is one fbc-oms built and authorized (`armed_oms`); the clock is paused
//! and held still.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::rc::Rc;

use armed_oms::{HELD, Oms, Reserved, churn, cid, order_caps, session_config, settle, watermark};
use common::Peer;
use common::ScriptedWs;
use exec_toy::{INST_A, ToyExec, ToySigner};
use fbc_core::{
    AccountSummary, AckLevel, AssetKey, Channel, ClientOrderId, ConfigError, CtxCall, DecodeError,
    DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt, EndpointPlan, Envelope, ExecCodec,
    ExecEndpoint, ExecEvent, ExecSink, FieldSpec, HttpPlan, HttpResponse, HttpTag, Inbound,
    InboundSpans, InstrumentSpecDraft, ItemRef, Lots, MdCodec, MonoNs, NewOrder, NotSentReason,
    OpKind, OrderKind, PathStamps, RateCharge, RawFrame, RpcId, Secrets, Side, SpecTable, StreamId,
    SubmitHandle, SubmitOutcome, Subscription, SymbolError, Ticks, Tif, TimerTag, TrafficClass,
    VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, WallNs, WireSlice,
};
use fbc_journal::{JournalSink, Record, Recorded, WriteRes};
use fbc_oms::{Authorization, CancelChoice, OrderOp};
use fbc_runtime::{ExecHandler, ExecOrders, ExecSession, InFlight, Journal, ReservedBound};

type Handles = Rc<RefCell<Vec<SubmitHandle>>>;
type Events = Rc<RefCell<Vec<ExecEvent>>>;

/// Keeps each submission's handle and the events it hears.
struct Keep {
    handles: Handles,
    events: Events,
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.handles.borrow_mut().push(handle);
    }
}

/// Keeps every record the session offers, with its class.
#[derive(Default)]
struct Kept(Vec<(TrafficClass, Record)>);

impl JournalSink for Kept {
    fn record(&mut self, class: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        self.0.push((class, record.clone()));
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::Ok
    }
}

// ---------------------------------------------------------------------------------------------
// The conformance toy, and a frame that makes the session write more than the socket holds.
// ---------------------------------------------------------------------------------------------

/// The conformance toy's order entry as `armed_oms`'s [`HELD`] declares it, its codec also
/// taking `big|kb=<n>` from the venue as a request to write an `n` KiB frame: a write the peer,
/// once it stops reading, holds well past what the loopback socket buffers take.
struct Stalling;

static STALLING: Stalling = Stalling;

impl VenueFactory for Stalling {
    fn id(&self) -> &'static str {
        "TOY-STALLING"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        HELD.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        HELD.caps(cfg)
    }

    fn parse_fbc_common_symbol(&self, symbol: &str) -> Result<AssetKey, SymbolError> {
        HELD.parse_fbc_common_symbol(symbol)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        HELD.discover(cfg)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        HELD.plan_md(cfg, specs, subs)
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        HELD.md_codec(cfg, ep)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        HELD.plan_exec(cfg)
    }

    fn exec_codec(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let toy = ToyExec::new(Box::new(ToySigner));
        Some(Ok(Box::new(Big(toy))))
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        HELD.test_connection(cfg, creds)
    }
}

/// The toy's codec, and a `big|kb=<n>` frame answered with an `n` KiB frame of its own.
struct Big(ToyExec);

impl ExecCodec for Big {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.0.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.0.on_open(stream, ctx, fx);
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        self.0.encode(cmd, rpc, specs, ctx, t, fx)
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let big = match f {
            RawFrame::Text(text) => text.strip_prefix("big|kb="),
            RawFrame::Binary(_) => None,
        };
        let Some(kb) = big else {
            return self.0.on_frame(stream, f, scope, specs, sink, fx);
        };
        let kb: usize = kb.parse().unwrap();
        fx.push(Effect::Send {
            stream,
            frame: WireSlice::plain(vec![b'x'; kb * 1024]),
            rpc: None,
            class: TrafficClass::Normal,
            charge: RateCharge {
                weight: NonZeroU32::new(1).unwrap(),
                ..RateCharge::one(OpKind::Control, None)
            },
        });
        Ok(())
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, fbc_core::HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.0.on_http(tag, resp, scope, specs, sink, fx)
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        self.0.on_timer(tag, ctx, fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.0.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.0.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.0.redact_inbound(input)
    }
}

// ---------------------------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------------------------

/// A session of the stalling toy at `url`, its outbound queue bounded by `outbound` and its
/// requests in flight by `in_flight`, journaling into `journal`; its orders and what its
/// handler keeps.
fn session(
    url: &str,
    outbound: ReservedBound,
    in_flight: &InFlight,
    journal: &Rc<RefCell<Kept>>,
) -> (
    ExecSession<Keep>,
    fbc_runtime::ExecControl,
    ExecOrders,
    Handles,
    Events,
) {
    let mut config = session_config(url, &Reserved::default());
    config.venue = &STALLING;
    config.outbound = outbound;
    config.in_flight = in_flight.clone();
    let (handles, events) = (Handles::default(), Events::default());
    let keep = Keep {
        handles: Rc::clone(&handles),
        events: Rc::clone(&events),
    };
    let (mut session, control) = ExecSession::new(config, keep).unwrap();
    session.set_journal(Journal::new(Rc::clone(journal)));
    let orders = session.orders();
    (session, control, orders, handles, events)
}

fn bound(normal: u32, reserve: u32) -> ReservedBound {
    ReservedBound::new(normal, reserve).unwrap()
}

/// Keeps the paused clock from auto-advancing while the sockets are idle, until dropped.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// Answers the session's login, its cancel-on-disconnect arm (request 1) and its resync on
/// `peer`, then waits until the epoch takes places.
async fn armed_and_resynced(peer: &mut Peer, orders: &ExecOrders, events: &Events) {
    assert!(peer.recv().await.starts_with("auth|ts="));
    peer.send("auth|ok=1|token=toy-session-token");
    assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
    let wm = watermark(&peer.recv().await);
    peer.send("item|rpc=1|i=0|res=ok");
    peer.send(&format!("rsbegin|wm={wm}"));
    peer.send("rsend");
    settle(|| events.borrow().contains(&ExecEvent::ResyncEnd)).await;
    assert!(orders.may_place());
}

/// A buy of 10 lots at `px` the registry placed and the venue acknowledged as `vid`, told to
/// the registry directly, as no session here wrote it: an order the test may cancel.
fn rest(oms: &mut Oms, px: i64, vid: &str) -> ClientOrderId {
    let cid = cid();
    let order = NewOrder {
        cid,
        inst: INST_A,
        side: Side::Buy,
        kind: OrderKind::Limit { px: Ticks(px) },
        qty: Lots::new(10).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    };
    let cmd = oms.reg.place(order).unwrap();
    drop(oms.reg.authorize(armed_oms::ACCT, cmd).unwrap());
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(exec_toy::with_scope(|scope| scope.venue_order_id(vid)).unwrap()),
    };
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    oms.reg
        .on_outcome(cid, OrderOp::Place, &item, &accepted, MonoNs(1_000))
        .unwrap();
    cid
}

/// A cancel of the order `cid`, built and authorized.
fn cancel(oms: &mut Oms, cid: ClientOrderId) -> Authorization {
    let permit = oms.reg.cancellable(cid).unwrap();
    let CancelChoice::Send(cmd) = permit.cancel(&order_caps()) else {
        panic!("a cancel of an acknowledged order is sent");
    };
    oms.reg.authorize(armed_oms::ACCT, cmd).unwrap()
}

/// What became of submission `rpc`, once the session has had its turn.
async fn outcome(handles: &Handles, rpc: RpcId) -> Result<(), NotSentReason> {
    settle(|| handles.borrow().iter().any(|h| h.rpc == rpc)).await;
    let handles = handles.borrow();
    let handle = handles.iter().find(|h| h.rpc == rpc).unwrap();
    handle
        .receipt
        .as_ref()
        .map(|_| ())
        .map_err(|reason| *reason)
}

/// The next frame `peer` reads starts with `kind|rpc=<rpc>|`.
async fn written(peer: &mut Peer, kind: &str, rpc: RpcId) {
    let frame = peer.recv().await;
    let head = format!("{kind}|rpc={}|", rpc.0);
    assert!(frame.starts_with(&head), "{frame} is not {head}");
}

/// The write results the session journaled for request `rpc`, with their class.
fn write_results(journal: &Rc<RefCell<Kept>>, rpc: RpcId) -> Vec<(TrafficClass, WriteRes)> {
    let journal = journal.borrow();
    let results = journal.0.iter().filter_map(|(class, record)| match record {
        Record::WriteResult {
            rpc: Some(id),
            result,
            ..
        } if *id == rpc => Some((*class, *result)),
        _ => None,
    });
    results.collect()
}

/// Whether the session journaled an outbound frame for request `rpc`.
fn wrote(journal: &Rc<RefCell<Kept>>, rpc: RpcId) -> bool {
    let journal = journal.borrow();
    let outbound =
        |record: &Record| matches!(record, Record::Outbound { rpc: Some(id), .. } if *id == rpc);
    journal.0.iter().any(|(_, record)| outbound(record))
}

const BIG_KB: usize = 65_536;

// ---------------------------------------------------------------------------------------------
// The outbound queue.
// ---------------------------------------------------------------------------------------------

/// The connection's queue holds two Normal commands and one more for Safety traffic. The peer
/// stops reading under a 64 MiB frame, so the session's write waits and what is submitted
/// meanwhile waits for the connection: two places fill the Normal bound, a third place is
/// `NotSent(Backpressure)`, a cancel takes the reserve, and a second cancel, the reserve full
/// too, is `NotSent(Backpressure)`. Nothing is reported while the write waits. Once the peer
/// reads again it hears the frame, the two places and the cancel, and nothing of either refused
/// command, whose refusals are journaled as their write results under their own class.
#[tokio::test(start_paused = true)]
async fn a_normal_command_past_the_outbound_bound_is_not_sent_backpressure_while_a_cancel_uses_the_reserve()
 {
    let thaw = freeze();
    let mut server = ScriptedWs::start().await;
    let journal = Rc::new(RefCell::new(Kept::default()));
    let in_flight = InFlight::new(bound(64, 64));
    let (mut session, control, orders, handles, events) =
        session(&server.url(), bound(2, 1), &in_flight, &journal);
    let mut oms = Oms::armed();
    let watch = Rc::clone(&handles);
    let script = async move {
        let handles = watch;
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let other = rest(&mut oms, 130_830, "V-3");
        peer.send(&format!("big|kb={BIG_KB}"));
        let release = peer.hold();
        churn().await;

        let (first, second) = (oms.place(), oms.place());
        let first = orders.submit(first).unwrap();
        let second = orders.submit(second).unwrap();
        let past = orders.submit(oms.place()).unwrap();
        let cancelled = orders.submit(oms.cancel()).unwrap();
        let full = orders.submit(cancel(&mut oms, other)).unwrap();
        churn().await;
        // The write still waits: no command has had its turn.
        assert!(handles.borrow().is_empty(), "{:?}", handles.borrow());

        drop(release);
        assert_eq!(peer.recv().await.len(), BIG_KB * 1024);
        written(&mut peer, "place", first).await;
        written(&mut peer, "place", second).await;
        written(&mut peer, "cancel", cancelled).await;
        let backpressure = Err(NotSentReason::Backpressure);
        assert_eq!(outcome(&handles, first).await, Ok(()));
        assert_eq!(outcome(&handles, second).await, Ok(()));
        assert_eq!(outcome(&handles, past).await, backpressure);
        assert_eq!(outcome(&handles, cancelled).await, Ok(()));
        assert_eq!(outcome(&handles, full).await, backpressure);
        churn().await;
        assert!(peer.quiet());
        drop(control);
        (past, full)
    };
    let (run, (past, full)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    // Each refused command reported once, journaled once as its write result under its class,
    // and never written.
    let refused = WriteRes::NotSent(NotSentReason::Backpressure);
    assert_eq!(
        write_results(&journal, past),
        [(TrafficClass::Normal, refused)]
    );
    assert_eq!(
        write_results(&journal, full),
        [(TrafficClass::Safety, refused)]
    );
    assert!(!wrote(&journal, past) && !wrote(&journal, full));
    assert_eq!(handles.borrow().len(), 5);
    assert_eq!(session.counters().backpressure_refusals, 2);
    assert_eq!(session.counters().in_flight_refusals, 0);
}

// ---------------------------------------------------------------------------------------------
// The requests in flight.
// ---------------------------------------------------------------------------------------------

/// The table holds two Normal requests in flight and one more for Safety traffic. Two places go
/// out unanswered and fill the Normal bound; a third place and an order query (a control
/// command, normal traffic) are `NotSent(RateBudget)` with nothing written, while a cancel
/// still goes out, from the reserve; a second cancel, the reserve full too, is refused. Once the
/// venue answers the first place, the cancel and the second place still fill the Normal bound,
/// so a place is refused; once it answers the second, a place goes out again. When the session
/// ends, the requests it still had in flight are given back to the table.
#[tokio::test(start_paused = true)]
async fn a_normal_request_past_the_in_flight_bound_is_not_sent_rate_budget_while_a_cancel_still_goes_out()
 {
    let thaw = freeze();
    let mut server = ScriptedWs::start().await;
    let journal = Rc::new(RefCell::new(Kept::default()));
    let in_flight = InFlight::new(bound(2, 1));
    let (mut session, control, orders, handles, events) =
        session(&server.url(), bound(64, 64), &in_flight, &journal);
    let mut oms = Oms::armed();
    let table = in_flight.clone();
    let watch = Rc::clone(&handles);
    let script = async move {
        let handles = watch;
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        // The arm was answered: nothing is in flight.
        assert_eq!(table.held(), 0);
        let other = rest(&mut oms, 130_830, "V-3");
        let budget = Err(NotSentReason::RateBudget);

        let first = orders.submit(oms.place()).unwrap();
        written(&mut peer, "place", first).await;
        let second = orders.submit(oms.place()).unwrap();
        written(&mut peer, "place", second).await;
        assert_eq!(table.held(), 2);

        let past = orders.submit(oms.place()).unwrap();
        assert_eq!(outcome(&handles, past).await, budget);
        let query = fbc_oms::ControlCommand::FeeQuery;
        let queried = orders.submit_control(query).unwrap();
        assert_eq!(outcome(&handles, queried).await, budget);
        churn().await;
        assert!(peer.quiet());

        let cancelled = orders.submit(oms.cancel()).unwrap();
        written(&mut peer, "cancel", cancelled).await;
        assert_eq!(outcome(&handles, cancelled).await, Ok(()));
        assert_eq!(table.held(), 3);
        let full = orders.submit(cancel(&mut oms, other)).unwrap();
        assert_eq!(outcome(&handles, full).await, budget);
        churn().await;
        assert!(peer.quiet());

        // Answers give their places back: with the cancel alone in flight, a place goes out
        // again.
        peer.send(&format!("item|rpc={}|i=0|res=ok", first.0));
        settle(|| table.held() == 2).await;
        let held = orders.submit(oms.place()).unwrap();
        assert_eq!(outcome(&handles, held).await, budget);
        peer.send(&format!("item|rpc={}|i=0|res=ok", second.0));
        settle(|| table.held() == 1).await;
        let again = orders.submit(oms.place()).unwrap();
        written(&mut peer, "place", again).await;
        assert_eq!(outcome(&handles, again).await, Ok(()));
        assert_eq!(table.held(), 2);
        drop(control);
        [past, queried, full, held]
    };
    let (run, refused) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    for rpc in refused {
        assert!(!wrote(&journal, rpc));
    }
    assert_eq!(session.counters().in_flight_refusals, 4);
    assert_eq!(session.counters().backpressure_refusals, 0);
    // The session ended: what it still had in flight is the table's again.
    assert_eq!(in_flight.held(), 0);
}

/// While the session waits for its connection to open, a fee query fills the Normal bound of a
/// queue with no reserve and a second is refused. The first is `NotSent(Disconnected)`, as a
/// command submitted with no authenticated epoch is (0057), and the second
/// `NotSent(Backpressure)`, its refusal journaled as its write result under Normal, neither
/// written.
#[tokio::test(start_paused = true)]
async fn a_refusal_waiting_while_the_session_connects_is_reported_backpressure_and_journaled() {
    let thaw = freeze();
    let (addr, _accepts) = common::hanging().await;
    let journal = Rc::new(RefCell::new(Kept::default()));
    let in_flight = InFlight::new(bound(64, 64));
    let url = format!("ws://{addr}/");
    let (mut session, control, orders, handles, _) =
        session(&url, bound(1, 0), &in_flight, &journal);
    let watch = Rc::clone(&handles);
    let script = async move {
        let handles = watch;
        let fees = || fbc_oms::ControlCommand::FeeQuery;
        let queried = orders.submit_control(fees()).unwrap();
        let past = orders.submit_control(fees()).unwrap();
        let disconnected = Err(NotSentReason::Disconnected);
        assert_eq!(outcome(&handles, queried).await, disconnected);
        let backpressure = Err(NotSentReason::Backpressure);
        assert_eq!(outcome(&handles, past).await, backpressure);
        drop(control);
        (queried, past)
    };
    let (run, (queried, past)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    let refused = WriteRes::NotSent(NotSentReason::Backpressure);
    assert_eq!(
        write_results(&journal, past),
        [(TrafficClass::Normal, refused)]
    );
    assert!(write_results(&journal, queried).is_empty());
    assert_eq!(handles.borrow().len(), 2);
    assert_eq!(session.counters().backpressure_refusals, 1);
}

/// An authorization for another account than the session's is refused as it is submitted
/// (0057) and takes no place in the queue: with room for one Normal command, a fee query
/// submitted next still waits as a command, not as a refusal.
#[tokio::test(start_paused = true)]
async fn an_authorization_for_another_account_takes_no_place_in_the_queue() {
    let mut config = session_config("ws://127.0.0.1:9/", &Reserved::default());
    config.acct = fbc_core::AccountKey::new(9);
    config.outbound = bound(1, 0);
    let (handles, events) = (Handles::default(), Events::default());
    let keep = Keep { handles, events };
    let (session, _control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let refused = orders.submit(oms.place());
    assert_eq!(refused, Err(fbc_runtime::SubmitRefusal::OtherAccount));
    assert!(
        orders
            .submit_control(fbc_oms::ControlCommand::FeeQuery)
            .is_ok()
    );
    assert_eq!(session.counters().backpressure_refusals, 0);
}
