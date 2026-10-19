//! FBC-e5p's done line: the reconnect storm that desynced the Java stack's order map (340
//! reconnects, [`STORM_RECONNECTS`]; FBC-jcy plays it against market data) played against
//! fbc-runtime's order-entry session for the conformance toy, on a paused clock, with every order
//! command built and authorized by fbc-oms and every event the session reports applied to it as
//! a live consumer applies them (decisions 0005, 0013 rule 1, 0023, 0025, 0058, 0080).
//!
//! The stub keeps no venue state: the script's responders (decision 0047) hold the venue's book
//! as the script states it, every connection's resync reply states the orders resting and the
//! position, and the last of those replies is what fbc-oms is compared with at the end.
//!
//! Each of the 341 connections opens alike: the authentication acknowledged, the session's
//! cancel-on-disconnect arm accepted, and a resync stating the venue's book. From the second
//! connection on (the owner's Start follows the first resync), the moment the epoch
//! authenticates, before the arm is answered, the consumer submits a place and, where the last
//! epoch left an order resting that fbc-oms may amend, an amend of it: the session refuses both
//! `NotSent(Disconnected)` with nothing written. The order the last epoch left resting
//! survives the stub's close (a server close, which 0080 says the venue's cancel-on-disconnect
//! may not cover), so the resync shows it, the session tells the consumer it is unprotected, and
//! the consumer cancels it before the epoch places. Then each epoch does one thing, in a cycle
//! of five, and the stub closes the connection:
//!
//! - a place, accepted and resting;
//! - a place the stub reads and never answers, which the venue never got: `Unknown` at its
//!   deadline while the session waits to reconnect, absent from the next resync, so Lost;
//! - a place the stub reads and never answers, which the venue holds: `Unknown` the same way,
//!   shown resting by the next resync, which resolves it, and cancelled there;
//! - a place accepted and partly filled, 5 lots, so the position moves;
//! - a place accepted and then amended to a new price, the toy giving the order a new venue id.
//!
//! On the last connection, which stays open, the consumer's cancel of the order the epoch
//! before left (the amended one) goes out and is left unanswered: the order still rests, as
//! the last resync says, and its market is held. Reconnects are paced at a 6 s floor and five
//! attempts per 60 s, so every unanswered request's 5 s deadline passes while the session
//! waits, and the clock moves only there: the test moves it to each next attempt the pacing
//! allows once the epoch has ended, and nothing else moves it.

mod order_entry;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::FutureExt;

use fbc_conformance::toy::{self, EXEC_URL_KEY, INST_A, OWN_NS, SYMBOL_A, TOY_TOKEN};
use fbc_conformance::{
    ConnRecord, Frame, HttpRouter, Responder, STORM_RECONNECTS, Step, StubServer, WsScript,
    check_pacing,
};
use fbc_core::{
    AccountLease, AckLevel, CancelReason, CidMatch, CidMint, ClientOrderId, ConnKey, ConnState,
    Envelope, ExecEvent, ItemRef, Lots, MarketLease, MonoNs, NamespaceLease, NotSentReason,
    OrderCaps, RpcId, Secrets, Side, SignedLots, SubmitHandle, SubmitOutcome, Ticks, VenueConfig,
    VenueFactory, VenueOrderSnapshot, WallNs, encode_cid,
};
use fbc_oms::{
    Admission, CancelChoice, FillLedger, FillTime, LadderConfig, LeaseKeys, Leases, LedgerConfig,
    MarketCapsConfig, OrdState, OrderKey, OrderOp, PermittedCommand, PositionCheck, PreTradeCaps,
    Registry, ResyncSnapshot, TerminalKind,
};
use fbc_runtime::{
    Connector, ExecCounters, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, IngestClock,
    ProxyConfig, RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use order_entry::{ACCT, Counting, LeaseDir, TOY, answer, buy, field};

/// The last connection's number: the storm's 340 reconnects make 341 connections.
const LAST: usize = STORM_RECONNECTS;
/// Every order here is a buy of 25 lots ([`buy`]) at this price.
const QTY: i64 = 25;
const PX: i64 = 130_865;
/// The price the epochs that amend amend their order to.
const AMENDED_PX: i64 = 130_864;
/// The price the amend each epoch submits before its arm is answered asks for.
const EARLY_PX: i64 = 130_863;
/// The lots each partly filled order fills.
const FILLED: i64 = 5;
/// The registry's inventory and resting caps on [`INST_A`], in lots: room for every order here.
const CAP: i64 = 1_000;
/// How long, by the wall clock, a wait lasts before the test fails.
const WATCHDOG: Duration = Duration::from_secs(60);

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// The pacing: a 6 s floor after a drop (past the toy's 5 s request deadline,
/// [`toy::RPC_TIMEOUT`]), at most five attempts in any 60 s, a 5 s attempt deadline.
fn pacing() -> ReconnectPacing {
    ReconnectPacing::new(secs(6), secs(48), 5, secs(60), secs(5)).unwrap()
}

/// When attempt `n` starts after the first under [`pacing`], every epoch ending on the instant
/// it opened (the clock moves only between epochs): the floor after each, and a fresh window
/// after each fifth.
fn start_of(n: usize) -> Duration {
    let n = n as u64;
    secs(60 * (n / 5) + 6 * (n % 5))
}

/// What epoch `e` does once it may place.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Kind {
    /// A place, accepted, resting.
    Rests,
    /// A place read and never answered, which the venue never got.
    LostInFlight,
    /// A place read and never answered, which the venue holds.
    KeptInFlight,
    /// A place accepted and filled [`FILLED`] lots.
    PartFilled,
    /// A place accepted, then amended to [`AMENDED_PX`].
    Amended,
    /// The last epoch places nothing.
    Last,
}

fn kind(e: usize) -> Kind {
    if e == LAST {
        return Kind::Last;
    }
    match e % 5 {
        0 => Kind::Rests,
        1 => Kind::LostInFlight,
        2 => Kind::KeptInFlight,
        3 => Kind::PartFilled,
        _ => Kind::Amended,
    }
}

/// Whether an order of ours rests at the venue as epoch `e` opens: the one epoch `e - 1` left.
fn rests_into(e: usize) -> bool {
    e > 0 && kind(e - 1) != Kind::LostInFlight
}

// ---------------------------------------------------------------------------------------------
// The venue, as the script states it.
// ---------------------------------------------------------------------------------------------

/// An order of ours resting at the venue, as the toy's wire spells it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Resting {
    cid: String,
    vid: String,
    px: i64,
    cum: i64,
}

/// What a resync reply states: the order of ours resting, if any, and the position.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Stated {
    resting: Option<Resting>,
    position: i64,
}

/// The book the script's responders state and change as they answer.
#[derive(Default)]
struct Venue {
    now: Stated,
    seq: u64,
    vids: u64,
    /// What the last resync reply stated.
    last_stated: Option<Stated>,
}

impl Venue {
    fn vid(&mut self) -> String {
        self.vids += 1;
        format!("toy-{}", self.vids)
    }

    fn seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// An order event of `o` in state `st` (with its fields), under the next sequence.
    fn order(&mut self, o: &Resting, st: &str) -> String {
        let seq = self.seq();
        format!(
            "order|cid={}|vid={}|sym={SYMBOL_A}|side=B|st={st}|cum={}|px={}|qty={QTY}|po=1|ro=0\
             |seq={seq}",
            o.cid, o.vid, o.cum, o.px
        )
    }
}

type Book = Arc<Mutex<Venue>>;

fn lock(book: &Book) -> MutexGuard<'_, Venue> {
    book.lock().unwrap_or_else(|e| e.into_inner())
}

/// Refuses the frame a responder read unless `holds`, saying what was expected.
fn ensure(holds: bool, expected: &str) -> Result<(), String> {
    holds
        .then_some(())
        .ok_or_else(|| format!("expected {expected}"))
}

/// The resync reply: the book as it stands, remembered as the last stated.
fn resync(book: &Book) -> Responder {
    let book = book.clone();
    answer("resync", move |r| {
        let mut v = lock(&book);
        let mut out = vec![format!("rsbegin|wm={}", field(r, "ts")?)];
        if let Some(o) = &v.now.resting {
            out.push(format!(
                "rsorder|cid={}|vid={}|sym={SYMBOL_A}|side=B|st=open|px={}|qty={QTY}|cum={}\
                 |po=1|ro=0",
                o.cid, o.vid, o.px, o.cum
            ));
        }
        out.push(format!("rspos|sym={SYMBOL_A}|qty={}", v.now.position));
        out.push("rsend".into());
        v.last_stated = Some(v.now.clone());
        Ok(out)
    })
}

/// The answer to the cancel of the order resting: accepted and cancelled when `answered`,
/// otherwise read and left unanswered, the order still resting.
fn cancel(book: &Book, answered: bool) -> Responder {
    let book = book.clone();
    answer("cancel", move |r| {
        let mut v = lock(&book);
        let o = v
            .now
            .resting
            .clone()
            .ok_or("a cancel with nothing resting")?;
        let names = field(r, "vid").ok() == Some(o.vid.clone())
            || field(r, "cid").ok() == Some(o.cid.clone());
        ensure(names, "a cancel of the order resting")?;
        if !answered {
            return Ok(Vec::new());
        }
        v.now.resting = None;
        let canceled = v.order(&o, "canceled|why=requested");
        Ok(vec![
            format!("item|rpc={}|i=0|res=ok", field(r, "rpc")?),
            canceled,
        ])
    })
}

/// The answer to epoch's place of `kind`.
fn place(book: &Book, kind: Kind) -> Responder {
    let book = book.clone();
    answer("place", move |r| {
        let mut v = lock(&book);
        let (rpc, cid) = (field(r, "rpc")?, field(r, "cid")?);
        ensure(
            v.now.resting.is_none(),
            "nothing of ours resting at a place",
        )?;
        if kind == Kind::LostInFlight {
            return Ok(Vec::new());
        }
        let vid = v.vid();
        let mut o = Resting {
            cid,
            vid: vid.clone(),
            px: PX,
            cum: 0,
        };
        let mut out = Vec::new();
        if kind != Kind::KeptInFlight {
            out.push(format!("item|rpc={rpc}|i=0|res=ok|vid={vid}"));
            out.push(v.order(&o, "open"));
        }
        if kind == Kind::PartFilled {
            o.cum = FILLED;
            let seq = v.seq();
            out.push(format!(
                "fill|fid=f{seq}|vid={vid}|cum={FILLED}|cid={}|sym={SYMBOL_A}|side=B|px={PX}\
                 |qty={FILLED}|liq=M|fee=100|fa=USDC|pnl=0|fund=0|seq={seq}",
                o.cid
            ));
            out.push(v.order(&o, "open"));
            v.now.position += FILLED;
        }
        v.now.resting = Some(o);
        Ok(out)
    })
}

/// The answer to the amend of the order resting: accepted, and the order event reporting it
/// replaced under a new venue id (`AmendAck::ReplacedEvent`).
fn amend(book: &Book) -> Responder {
    let book = book.clone();
    answer("amend", move |r| {
        let mut v = lock(&book);
        let mut o = v
            .now
            .resting
            .clone()
            .ok_or("an amend with nothing resting")?;
        ensure(field(r, "vid")? == o.vid, "an amend of the order resting")?;
        o.px = field(r, "px")?.parse().map_err(|_| "a price")?;
        let nvid = v.vid();
        let amended = v.order(&o, &format!("amended|nvid={nvid}"));
        o.vid = nvid;
        v.now.resting = Some(o);
        Ok(vec![
            format!("item|rpc={}|i=0|res=ok", field(r, "rpc")?),
            amended,
        ])
    })
}

/// The storm's script, and where each of its barriers ends, in order.
#[derive(Default)]
struct Storm {
    steps: Vec<Step>,
    barriers: Vec<usize>,
}

impl Storm {
    fn respond(&mut self, conn: usize, with: Responder) {
        self.steps.push(Step::Respond { conn, with });
    }

    /// A barrier: once it has passed, the session has handed every frame before it to the glue.
    fn barrier(&mut self, conn: usize) {
        self.steps.push(Step::Barrier { conn });
        self.barriers.push(self.steps.len());
    }
}

fn storm(book: &Book) -> Storm {
    let auth = answer("auth", |_| Ok(vec![format!("auth|ok=1|token={TOY_TOKEN}")]));
    let arm = answer("cod", |r| {
        Ok(vec![format!("item|rpc={}|i=0|res=ok", field(r, "rpc")?)])
    });
    let mut s = Storm::default();
    for e in 0..=LAST {
        s.steps.push(Step::Accept);
        s.respond(e, auth.clone());
        s.respond(e, arm.clone());
        s.respond(e, resync(book));
        s.barrier(e);
        if rests_into(e) {
            s.respond(e, cancel(book, e < LAST));
            s.barrier(e);
        }
        if kind(e) != Kind::Last {
            s.respond(e, place(book, kind(e)));
            s.barrier(e);
        }
        if kind(e) == Kind::Amended {
            s.respond(e, amend(book));
            s.barrier(e);
        }
        if e < LAST {
            s.steps.push(Step::Close { conn: e });
        }
    }
    s
}

// ---------------------------------------------------------------------------------------------
// The consumer: fbc-oms and the glue applying the session's events to it.
// ---------------------------------------------------------------------------------------------

/// An order command the glue submitted.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Op {
    Place,
    Amend { px: Ticks, qty: Lots },
    Cancel,
}

impl Op {
    fn order_op(self, rpc: RpcId) -> OrderOp {
        match self {
            Op::Place => OrderOp::Place,
            Op::Amend { .. } => OrderOp::Amend(rpc),
            Op::Cancel => OrderOp::Cancel(rpc),
        }
    }

    /// Whether it adds: a place or an amend, which wait for the arm and the resync.
    fn adds(self) -> bool {
        self != Op::Cancel
    }
}

/// What the glue saw, in the order it saw it, each under the epoch it came on.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Mark {
    Authenticated,
    /// The venue accepted the session's own cancel-on-disconnect arm, request `rpc`.
    ArmAccepted(RpcId),
    /// The registry applied the epoch's resync.
    ResyncApplied,
    /// The session reported the submission `rpc` sent.
    Written(Op, RpcId),
    /// The session reported the submission `rpc` not sent.
    NotSent(Op, RpcId, NotSentReason),
}

/// A submission: the order and the command.
#[derive(Copy, Clone, Debug)]
struct Sub {
    cid: ClientOrderId,
    op: Op,
}

struct State {
    reg: Registry,
    ledger: FillLedger,
    caps: OrderCaps,
    ladder: LadderConfig,
    orders: Option<ExecOrders>,
    mint: CidMint,
    sent: HashMap<RpcId, Sub>,
    collecting: Option<ResyncSnapshot>,
    authenticated_at: Option<MonoNs>,
    last: MonoNs,
    /// The epoch of the last event.
    epoch: u32,
    /// Whether the owner's Start was given.
    started: bool,
    marks: Vec<(u32, Mark)>,
    resynced: usize,
    /// How many epochs the session reported ended.
    ended: usize,
    checks: Vec<PositionCheck>,
    /// The orders the resyncs ended Lost.
    lost: Vec<ClientOrderId>,
    /// Every order the scenario placed, in order (not the early ones).
    placed: Vec<ClientOrderId>,
    /// Every early submission: its request and command.
    early: Vec<(RpcId, Op)>,
    /// The orders the session reported unprotected, by epoch.
    told: Vec<(u32, Vec<ClientOrderId>)>,
}

impl State {
    fn mark(&mut self, m: Mark) {
        self.marks.push((self.epoch, m));
    }

    /// Authorizes `cmd` and submits it to the session.
    fn submit(&mut self, cid: ClientOrderId, op: Op, cmd: PermittedCommand, early: bool) -> RpcId {
        let auth = self.reg.authorize(ACCT, cmd).unwrap();
        let rpc = self.orders.as_ref().unwrap().submit(auth).unwrap();
        self.sent.insert(rpc, Sub { cid, op });
        if early {
            self.early.push((rpc, op));
        }
        rpc
    }

    /// The place, built and authorized by the registry, submitted.
    fn place(&mut self, early: bool) -> ClientOrderId {
        let cid = self.mint.mint().unwrap();
        let cmd = self.reg.place(buy(cid)).unwrap();
        self.submit(cid, Op::Place, cmd, early);
        cid
    }

    /// The amend of `cid` to `px`, built and authorized by the registry and submitted; `None`
    /// where the registry gives the order no amend permit.
    fn amend(&mut self, cid: ClientOrderId, px: i64, early: bool) -> Option<RpcId> {
        let qty = Lots::new(QTY).unwrap();
        let live = self.reg.live(cid).ok()?;
        let cmd = live.amend(&self.caps, Ticks(px), qty, false).unwrap();
        let op = Op::Amend { px: Ticks(px), qty };
        Some(self.submit(cid, op, cmd, early))
    }

    fn on_event(&mut self, env: Envelope<ExecEvent>) {
        let now = env.stamp.recv_mono;
        self.last = now;
        self.epoch = env.stamp.conn.epoch;
        let key = OrderKey {
            venue: env.venue_seq,
            ingest: env.stamp.ingest_seq,
        };
        match env.body {
            ExecEvent::Conn {
                state: ConnState::Authenticated,
                ..
            } => {
                self.authenticated_at = Some(now);
                self.mark(Mark::Authenticated);
                // Before the arm is answered: a place, and an amend of the order the last
                // epoch left where the registry may amend it.
                if self.started {
                    if let Some(&cid) = self.placed.last() {
                        self.amend(cid, EARLY_PX, true);
                    }
                    self.place(true);
                }
            }
            ExecEvent::ResyncBegin { watermark } => {
                self.collecting = Some(ResyncSnapshot {
                    watermark,
                    requested_at: self.authenticated_at.expect("authenticated"),
                    orders: Vec::new(),
                    positions: Vec::new(),
                });
            }
            ExecEvent::ResyncOrder(o) => self.collecting().orders.push(o),
            ExecEvent::ResyncPosition { inst, qty, .. } => {
                self.collecting().positions.push((inst, qty));
            }
            ExecEvent::ResyncEnd => {
                let snap = self.collecting.take().expect("a resync begun");
                let report = self.reg.resync(&self.ladder, &self.caps, &snap, key);
                let report = report.expect("the registry takes the resync");
                assert!(report.untracked.is_empty(), "{report:?}");
                self.checks.extend(report.checks);
                self.lost.extend(report.ladder.lost);
                self.resynced += 1;
                self.mark(Mark::ResyncApplied);
            }
            ExecEvent::Outcome { rpc, item, outcome } => match self.sent.get(&rpc).copied() {
                Some(sub) => {
                    let whole = ItemRef {
                        idx: 0,
                        cid: Some(sub.cid),
                        vid: None,
                    };
                    let item = item.unwrap_or(whole);
                    let op = sub.op.order_op(rpc);
                    let applied = self.reg.on_outcome(sub.cid, op, &item, &outcome, now);
                    applied.expect("the registry takes the outcome");
                }
                // The session's own request: its cancel-on-disconnect arm.
                None => {
                    let accepted = SubmitOutcome::Accepted {
                        ack: AckLevel::Final,
                    };
                    assert_eq!(outcome, accepted, "the arm's outcome");
                    self.mark(Mark::ArmAccepted(rpc));
                }
            },
            ExecEvent::Order(u) => {
                self.reg.apply_update(&u, key);
            }
            ExecEvent::Fill(f) => {
                let time = env.exch_ts.map(|exch| FillTime {
                    exch,
                    kind: env.exch_ts_kind,
                    aligned: WallNs(exch.0),
                });
                match self.ledger.admit(&f, time, now) {
                    Admission::Apply(accepted) => {
                        self.reg
                            .apply_fill(accepted)
                            .expect("the registry counts it");
                    }
                    other => panic!("a live fill not applied: {other:?}"),
                }
            }
            _ => {}
        }
    }

    fn collecting(&mut self) -> &mut ResyncSnapshot {
        self.collecting.as_mut().expect("a resync begun")
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let rpc = handle.rpc;
        let sub = self.sent[&rpc];
        // An amend the session took is in flight until its outcome resolves it, a not-sent one
        // included.
        if let Op::Amend { px, qty } = sub.op {
            self.reg
                .amend_sent(sub.cid, px, qty, rpc, self.last)
                .unwrap();
        }
        match handle.receipt {
            Ok(_) => {
                match sub.op {
                    Op::Place => {
                        let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                        let wall = WallNs(i64::try_from(wall.as_nanos()).unwrap());
                        self.reg.placement_sent(sub.cid, self.last, wall).unwrap();
                    }
                    Op::Cancel => {
                        self.reg.cancel_sent(sub.cid, rpc, self.last).unwrap();
                    }
                    Op::Amend { .. } => {}
                }
                self.mark(Mark::Written(sub.op, rpc));
            }
            Err(reason) => {
                let item = ItemRef {
                    idx: 0,
                    cid: Some(sub.cid),
                    vid: None,
                };
                let outcome = SubmitOutcome::NotSent(reason);
                let op = sub.op.order_op(rpc);
                let applied = self.reg.on_outcome(sub.cid, op, &item, &outcome, self.last);
                applied.expect("the registry takes the not-sent outcome");
                self.mark(Mark::NotSent(sub.op, rpc, reason));
            }
        }
    }

    /// Cancels each order the session reports unprotected, through its cancel permit.
    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        let mut told = Vec::new();
        for o in orders {
            let Some(CidMatch::Ours(cid)) = o.cid else {
                panic!("an unprotected order not ours: {o:?}");
            };
            let caps = self.caps.clone();
            let permit = self.reg.cancellable(cid).expect("a cancel permit");
            let CancelChoice::Send(cmd) = permit.cancel(&caps) else {
                panic!("the registry holds {cid:?} ended");
            };
            self.submit(cid, Op::Cancel, cmd, false);
            told.push(cid);
        }
        self.told.push((self.epoch, told));
    }
}

#[derive(Clone)]
struct Glue(Rc<RefCell<State>>);

impl ExecHandler for Glue {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.0.borrow_mut().on_event(env);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.0.borrow_mut().on_submitted(handle);
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.0.borrow_mut().on_unprotected(orders);
    }

    fn on_epoch_end(&mut self, _key: ConnKey) {
        self.0.borrow_mut().ended += 1;
    }
}

// ---------------------------------------------------------------------------------------------
// The run.
// ---------------------------------------------------------------------------------------------

/// What the scenario drives the session with.
struct Run<'a> {
    st: Rc<RefCell<State>>,
    orders: ExecOrders,
    server: &'a StubServer,
    barriers: Vec<usize>,
    waited: usize,
    dir: &'a LeaseDir,
}

impl Run<'_> {
    /// Lets the session and the stub run, without moving the clock, until `done` holds; the test
    /// fails, naming `what`, once the script has failed or the watchdog's wall-clock time has
    /// passed.
    async fn until(&self, what: &str, done: impl Fn(&Run<'_>) -> bool) {
        let give_up = std::time::Instant::now() + WATCHDOG;
        while !done(self) {
            // A script that failed (a frame it did not expect) fails the test at once.
            if let Some(Err(e)) = self.server.finished().now_or_never() {
                panic!("never {what}: the script failed: {e}");
            }
            assert!(std::time::Instant::now() < give_up, "never {what}");
            tokio::task::yield_now().await;
        }
    }

    /// Waits until the script's next barrier has passed.
    async fn replied(&mut self) {
        let at = self.barriers[self.waited];
        self.waited += 1;
        self.until("replied", |r| r.server.played() >= at).await;
    }

    /// The owner's Start on [`INST_A`].
    fn start(&mut self) {
        let account = self.dir.account();
        let symbol = toy::specs().get(INST_A).unwrap().venue_symbol.clone();
        let market = MarketLease::acquire(&self.dir.0, TOY.id(), &account, &symbol).unwrap();
        let acct = AccountLease::acquire(&self.dir.0, TOY.id(), &account).unwrap();
        let leases = Leases::market(market).with_account(acct);
        let mut st = self.st.borrow_mut();
        st.reg.start(INST_A, leases).unwrap();
        st.started = true;
    }

    fn place(&mut self) -> ClientOrderId {
        let mut st = self.st.borrow_mut();
        let cid = st.place(false);
        st.placed.push(cid);
        cid
    }

    fn amend(&mut self, cid: ClientOrderId) {
        let sent = self.st.borrow_mut().amend(cid, AMENDED_PX, false);
        assert!(sent.is_some(), "no amend permit for {cid:?}");
    }
}

/// What the storm left.
struct Played {
    st: State,
    conns: Vec<ConnRecord>,
    counters: ExecCounters,
    /// The orders the session held unprotected as the run ended.
    held: Vec<VenueOrderSnapshot>,
    /// What the last resync reply stated.
    stated: Stated,
}

fn play_storm() -> Played {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let dir = LeaseDir::new();
    let caps = TOY.caps(&VenueConfig::new()).unwrap();
    let order = caps.exec.clone().unwrap().order;
    let account = dir.account();
    let symbol = toy::specs().get(INST_A).unwrap().venue_symbol.clone();
    let keys = LeaseKeys::new(TOY.id(), &account, &order).with_market(INST_A, symbol);
    let cap = Some(Lots::new(CAP).unwrap());
    let limits = MarketCapsConfig {
        inventory: cap,
        resting: cap,
    };
    let pre_trade = PreTradeCaps::new().with_market(INST_A, limits).unwrap();
    let mut reg = Registry::with_caps(pre_trade).with_lease_keys(keys);
    reg.bind_account(ACCT).unwrap();
    let ledger = LedgerConfig {
        max_age: secs(24 * 3_600),
        max_entries: 1_024,
    };
    let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
    let st = Rc::new(RefCell::new(State {
        reg,
        // Live fills only: the watermark is read by replayed fills alone.
        ledger: FillLedger::new(ledger, WallNs(0)).unwrap(),
        caps: order,
        // An order on the Unknown ladder that one trustworthy snapshot after it was sent does
        // not show is Lost.
        ladder: LadderConfig::new(secs(1), Duration::ZERO, secs(10), 1).unwrap(),
        orders: None,
        mint: CidMint::new(lease, 0, 0, WallNs(1)),
        sent: HashMap::new(),
        collecting: None,
        authenticated_at: None,
        last: MonoNs(0),
        epoch: 0,
        started: false,
        marks: Vec::new(),
        resynced: 0,
        ended: 0,
        checks: Vec::new(),
        lost: Vec::new(),
        placed: Vec::new(),
        early: Vec::new(),
        told: Vec::new(),
    }));
    let book = Book::default();
    let script = storm(&book);
    let end = script.steps.len();
    let (counters, conns, held) = rt.block_on(async {
        // A blocked thread keeps the paused clock from moving on its own: only the scenario
        // moves it, between epochs.
        let (thaw, frozen) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || frozen.recv());
        let server = StubServer::start(WsScript::new(script.steps), HttpRouter::new())
            .await
            .unwrap();
        let mut cfg = VenueConfig::new();
        cfg.insert(EXEC_URL_KEY, &server.ws_url("/exec"));
        let config = ExecSessionConfig {
            venue: &TOY,
            cfg,
            creds: Secrets::new(),
            acct: ACCT,
            rpc_ids: RpcIds::default(),
            ns: OWN_NS,
            specs: toy::specs(),
            connector: Connector::new(ProxyConfig::Direct),
            pacing: pacing(),
            clock: IngestClock::new(),
            nonces: Box::new(Counting(1)),
            nonce_source: fbc_journal::NonceSourceId(0),
            conn: 0,
            limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(0).unwrap()).unwrap(),
            write_stall: WriteStall::new(secs(3_600)).unwrap(),
            http_max_body: 1 << 20,
        };
        let (mut session, control) = ExecSession::new(config, Glue(st.clone())).unwrap();
        st.borrow_mut().orders = Some(session.orders());
        let mut run = Run {
            st: st.clone(),
            orders: session.orders(),
            server: &server,
            barriers: script.barriers,
            waited: 0,
            dir: &dir,
        };
        let driven = async {
            for e in 0..=LAST {
                // Authenticated, armed and resynced.
                run.replied().await;
                if e == 0 {
                    run.start();
                }
                // The order the last epoch left, cancelled.
                if rests_into(e) {
                    run.replied().await;
                }
                // Its market released by the cancel, except on the last epoch, whose cancel
                // goes unanswered.
                assert_eq!(run.orders.may_place_on(INST_A), e < LAST, "epoch {e}");
                if kind(e) != Kind::Last {
                    let cid = run.place();
                    run.replied().await;
                    if kind(e) == Kind::Amended {
                        run.amend(cid);
                        run.replied().await;
                    }
                }
                if e < LAST {
                    // The stub closes the connection; once the session has ended the epoch,
                    // the clock moves to the next attempt the pacing allows, so the deadline
                    // of every request still unanswered passes as the session waits.
                    run.until("the epoch ended", |r| r.st.borrow().ended == e + 1)
                        .await;
                    let first = server.connections()[0].accepted_at;
                    let next = first + start_of(e + 1);
                    let now = tokio::time::Instant::now();
                    assert!(next > now, "epoch {e}: the next attempt is due at once");
                    tokio::time::advance(next - now).await;
                }
            }
            run.until("played to its end", |r| r.server.played() >= end)
                .await;
            let held = run.orders.unprotected();
            drop(control);
            held
        };
        let (ran, held) = tokio::join!(session.run(), driven);
        drop(thaw);
        ran.unwrap();
        server.finished().await.unwrap();
        (session.counters(), server.connections(), held)
    });
    drop(rt);
    let st = Rc::into_inner(st).unwrap().into_inner();
    let stated = lock(&book).last_stated.clone().expect("a resync replied");
    Played {
        st,
        conns,
        counters,
        held,
        stated,
    }
}

// ---------------------------------------------------------------------------------------------
// What the run shows.
// ---------------------------------------------------------------------------------------------

/// The kind of a frame the stub received: its record's name.
fn kind_of(frame: &Frame) -> &str {
    match frame {
        Frame::Text(t) => t.split('|').next().unwrap_or_default(),
        Frame::Binary(_) => "binary",
    }
}

/// A frame's field `key`, which it must carry.
fn field_of(frame: &Frame, key: &str) -> String {
    field(frame, key).unwrap()
}

fn rpc_of(frame: &Frame) -> RpcId {
    RpcId(field_of(frame, "rpc").parse().unwrap())
}

/// The kinds of the frames epoch `e`'s connection should carry, in order.
fn expected_kinds(e: usize) -> Vec<&'static str> {
    let mut kinds = vec!["auth", "cod", "resync"];
    if rests_into(e) {
        kinds.push("cancel");
    }
    match kind(e) {
        Kind::Last => {}
        Kind::Amended => kinds.extend(["place", "amend"]),
        _ => kinds.push("place"),
    }
    kinds
}

fn wire(cid: ClientOrderId) -> String {
    let caps = toy::caps().exec.unwrap().order;
    encode_cid(&caps.client_id, cid).unwrap().to_string()
}

#[test]
fn the_340_reconnect_storm_resends_nothing_and_arms_and_resyncs_before_every_order() {
    let p = play_storm();
    let marks = &p.st.marks;

    // Connection attempts within the pacing: the floor after each drop, five per 60 s window,
    // none failed, and each epoch armed, nothing refused for want of protection.
    let starts: Vec<_> = p.conns.iter().map(|c| c.accepted_at).collect();
    assert_eq!(starts.len(), LAST + 1);
    check_pacing(&starts, &pacing()).unwrap();
    let since: Vec<_> = starts.iter().map(|t| t.duration_since(starts[0])).collect();
    assert_eq!(since, (0..=LAST).map(start_of).collect::<Vec<_>>());
    let c = &p.counters;
    let attempts = LAST as u64 + 1;
    assert_eq!((c.attempts, c.failed_attempts), (attempts, 0));
    assert_eq!(
        (c.arm_failures, c.decode_errors, c.refused_effects),
        (0, 0, 0)
    );
    // The market of an order resting from an earlier epoch was held until its cancel ended it,
    // and nothing was placed on it meanwhile.
    assert_eq!(c.unprotected_refusals, 0);

    // Each connection carried its authentication, one arm, one resync and then only the epoch's
    // own orders, in that order.
    for (e, conn) in p.conns.iter().enumerate() {
        let kinds: Vec<_> = conn.received.iter().map(kind_of).collect();
        assert_eq!(kinds, expected_kinds(e), "connection {e}");
    }
    assert_eq!(p.st.resynced, LAST + 1);

    // No place, amend or cancel frame written twice: every request id once, every order placed
    // once, amended at most once and cancelled at most once, and each written on the
    // connection of the epoch it was submitted on, never after a reconnect.
    let written: HashMap<RpcId, (u32, Op)> = marks
        .iter()
        .filter_map(|(e, m)| match m {
            Mark::Written(op, rpc) => Some((*rpc, (*e, *op))),
            _ => None,
        })
        .collect();
    let mut rpcs = HashSet::new();
    let (mut placed, mut amended, mut cancelled) = (HashSet::new(), HashSet::new(), HashSet::new());
    let mut orders_written = 0;
    for (e, conn) in p.conns.iter().enumerate() {
        for frame in &conn.received {
            let target = match kind_of(frame) {
                "place" => &mut placed,
                "amend" => &mut amended,
                "cancel" => &mut cancelled,
                _ => continue,
            };
            orders_written += 1;
            let rpc = rpc_of(frame);
            assert!(rpcs.insert(rpc), "request {rpc:?} written twice");
            let named = field(frame, "cid")
                .or_else(|_| field(frame, "vid"))
                .unwrap();
            assert!(
                target.insert(named.clone()),
                "{named} written twice: {frame:?}"
            );
            let (epoch, _) = written[&rpc];
            assert_eq!(
                epoch as usize, e,
                "request {rpc:?} written on another epoch"
            );
        }
    }
    assert_eq!(orders_written, written.len());
    assert_eq!(placed.len(), LAST);
    assert_eq!(amended.len(), LAST / 5);
    let rests = (1..=LAST).filter(|&e| rests_into(e)).count();
    assert_eq!(cancelled.len(), rests);

    // On every epoch the arm was accepted, under the request the stub read, and the resync
    // applied, each once, before any place or amend was written; the place and the amend
    // submitted the moment the epoch authenticated were refused, nothing written.
    for (e, conn) in p.conns.iter().enumerate() {
        let of_epoch: Vec<Mark> = marks
            .iter()
            .filter(|(at, _)| *at as usize == e)
            .map(|(_, m)| *m)
            .collect();
        let at = |want: &dyn Fn(&Mark) -> bool| -> Vec<usize> {
            of_epoch
                .iter()
                .enumerate()
                .filter(|(_, m)| want(m))
                .map(|(i, _)| i)
                .collect()
        };
        let arm_rpc = rpc_of(&conn.received[1]);
        let authenticated = at(&|m| *m == Mark::Authenticated);
        let armed = at(&|m| *m == Mark::ArmAccepted(arm_rpc));
        let resynced = at(&|m| *m == Mark::ResyncApplied);
        assert_eq!(authenticated.len(), 1, "epoch {e}: {of_epoch:?}");
        assert_eq!(armed.len(), 1, "epoch {e}: {of_epoch:?}");
        assert_eq!(resynced.len(), 1, "epoch {e}: {of_epoch:?}");
        let ready = armed[0].max(resynced[0]);
        let adds = at(&|m| matches!(m, Mark::Written(op, _) if op.adds()));
        assert!(adds.iter().all(|&i| i > ready), "epoch {e}: {of_epoch:?}");
        let refused = at(&|m| matches!(m, Mark::NotSent(..)));
        assert!(
            refused
                .iter()
                .all(|&i| authenticated[0] < i && i < armed[0]),
            "epoch {e}: {of_epoch:?}"
        );
        // From epoch 1 (after the owner's Start) a place, and an amend where the last epoch
        // left an order the registry may amend.
        let amendable =
            e > 0 && matches!(kind(e - 1), Kind::Rests | Kind::PartFilled | Kind::Amended);
        let early = usize::from(e > 0) + usize::from(amendable);
        assert_eq!(refused.len(), early, "epoch {e}: {of_epoch:?}");
    }
    let early_rpcs: HashSet<RpcId> = p.st.early.iter().map(|(rpc, _)| *rpc).collect();
    for (rpc, op) in &p.st.early {
        let refused = Mark::NotSent(*op, *rpc, NotSentReason::Disconnected);
        assert!(marks.iter().any(|(_, m)| *m == refused), "{rpc:?} {op:?}");
    }
    assert!(rpcs.is_disjoint(&early_rpcs));
    assert_eq!(p.st.early.len(), LAST + 3 * LAST / 5);
    assert_eq!(c.unready_refusals, p.st.early.len() as u64);

    // Every resync agreed with the inventory, and each order the venue never got was Lost.
    assert_eq!(p.st.checks.len(), LAST);
    assert!(
        p.st.checks
            .iter()
            .all(|c| matches!(c, PositionCheck::Agrees { inst: INST_A, .. })),
        "{:?}",
        p.st.checks
    );
    let placed_on = |k: Kind| -> Vec<ClientOrderId> {
        (0..LAST)
            .filter(|&e| kind(e) == k)
            .map(|e| p.st.placed[e])
            .collect()
    };
    assert_eq!(p.st.lost, placed_on(Kind::LostInFlight));
    assert_eq!(p.st.reg.lost(), (LAST / 5) as u64);
    // Every order resting as an epoch opened was told unprotected on that epoch, and only it.
    let told: Vec<_> = p.st.told.iter().filter(|(_, t)| !t.is_empty()).collect();
    assert_eq!(told.len(), rests);
    for (e, cids) in told {
        assert_eq!(*cids, vec![p.st.placed[*e as usize - 1]], "epoch {e}");
    }

    // Every order but the one the last resync shows ended as the script had it.
    let canceled = OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested));
    for (e, &cid) in p.st.placed.iter().enumerate().take(LAST - 1) {
        let want = match kind(e) {
            Kind::LostInFlight => OrdState::Terminal(TerminalKind::Lost),
            _ => canceled,
        };
        assert_eq!(p.st.reg.get(cid).unwrap().state(), want, "epoch {e}");
    }
    let disconnected = OrdState::Terminal(TerminalKind::NotSent(NotSentReason::Disconnected));
    for (rpc, op) in &p.st.early {
        if *op == Op::Place {
            let cid = p.st.sent[rpc].cid;
            assert_eq!(p.st.reg.get(cid).unwrap().state(), disconnected);
        }
    }

    // fbc-oms's open orders and position are those the last resync reply stated: the order the
    // epoch before it amended, still resting with its cancel unanswered, and the position the
    // partly filled orders built.
    let partly = (0..LAST).filter(|&e| kind(e) == Kind::PartFilled).count() as i64;
    let last = p.st.placed[LAST - 1];
    let o = p.stated.resting.clone().expect("an order resting");
    assert_eq!(p.stated.position, FILLED * partly);
    assert_eq!((o.cid.clone(), o.px, o.cum), (wire(last), AMENDED_PX, 0));
    let open: Vec<ClientOrderId> =
        p.st.sent
            .values()
            .map(|s| s.cid)
            .collect::<HashSet<_>>()
            .into_iter()
            .filter(|&cid| !p.st.reg.get(cid).unwrap().state().is_terminal())
            .collect();
    assert_eq!(open, vec![last]);
    let rec = p.st.reg.get(last).unwrap();
    assert_eq!(wire(rec.cid()), o.cid);
    assert_eq!(
        rec.vid().map(|v| v.as_str().to_owned()),
        Some(o.vid.clone())
    );
    assert_eq!(rec.px(), Some(Ticks(o.px)));
    assert_eq!(rec.qty(), Lots::new(QTY).unwrap());
    assert_eq!(rec.cum_venue(), Lots::new(o.cum).unwrap());
    assert_eq!(rec.resting(), Lots::new(QTY - o.cum).unwrap());
    let position = SignedLots(p.stated.position);
    assert_eq!(p.st.reg.position(INST_A), Some(position));
    assert_eq!(p.st.reg.inventory(INST_A), position);
    assert_eq!(
        p.st.reg.resting_on(INST_A, Side::Buy),
        Some(Lots::new(QTY).unwrap())
    );
    // Its market stays held: the venue has not shown it ended.
    let held: Vec<_> = p
        .held
        .iter()
        .map(|h| (h.cid, h.vid.as_str().to_owned()))
        .collect();
    assert_eq!(held, vec![(Some(CidMatch::Ours(last)), o.vid)]);
}
