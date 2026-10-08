//! What the order-entry checks share: fbc-runtime's order-entry session for the venue, run
//! against the stub server answering as the setup's [`OrderEntryStub`] says, on a paused clock
//! the check moves itself, with every order command built and authorized by an fbc-oms
//! registry armed on the setup's first instrument, as a live consumer's is (FBC-ob2).
//!
//! The clock is tokio's, paused, and a blocked thread keeps it from moving on its own while the
//! sockets are idle: a request's deadline passes only when a check moves the clock past it,
//! never while the stub's answer is still on its way.

use std::cell::{Cell, RefCell};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_core::{
    AccountKey, AccountLease, ClientOrderId, Envelope, ExecEvent, ItemRef, Lots, MarketLease,
    MonoNs, NewOrder, NonceBlock, NonceSource, OpKind, OrderCaps, OrderKind, OrderKindTag,
    RateLimit, RpcId, Side, SignedLots, SubmitHandle, SubmitOutcome, Ticks, VenueCaps,
    VenueFactory, VenueOrderId, WallNs, encode_cid,
};
use fbc_oms::{
    Authorization, LadderConfig, LeaseKeys, Leases, MarketCapsConfig, OrderKey, OrderOp,
    PreTradeCaps, Registry, ResyncSnapshot,
};
use fbc_runtime::{
    Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, IngestClock, ProxyConfig,
    RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use futures_util::FutureExt;

use super::harness::{Harness, NAMESPACE, Shape};
use super::stub::Answer;
use super::{Breach, Failure, Subject, Verdict};
use crate::script::{Frame, Step, WsScript};
use crate::server::StubServer;

/// The account the session trades and fbc-oms authorizes for.
const ACCT: AccountKey = AccountKey::new(1);
/// The registry's inventory and resting caps on the instrument, in orders of the harness's size:
/// room for every order a check places, whatever the instrument's minimum size (Codex
/// r4222138063).
const CAP_ORDERS: i64 = 8;
/// How far the clock moves at a time while a check waits for a deadline, for its first
/// [`FINE`]; [`COARSE`] after that, where a deadline that has not passed is long and only its
/// passing is waited for.
pub(crate) const STEP: Duration = Duration::from_millis(100);
const FINE: Duration = Duration::from_secs(30);
const COARSE: Duration = Duration::from_secs(1);
/// The longest a check waits for a deadline to pass: longer than any venue's request deadline.
pub(crate) const WAIT: Duration = Duration::from_secs(600);
/// How many times a check lets every task run, without moving the clock, before it gives up
/// waiting for something the stub's answer should bring.
const TURNS: usize = 100_000;
/// How many times a check lets every task run after the stub has answered, or the clock moved,
/// for what that brings to reach the session.
const CHURN: usize = 200;
/// The client ids a check's registry places under.
const CIDS: usize = 8;

/// The venue's order entry under the assumed setup, ready for a run of its session.
pub(crate) struct Live<'s> {
    pub h: Harness<'s>,
    venue: &'static dyn VenueFactory,
    subject: &'s Subject<'static>,
    pub order: OrderCaps,
}

impl<'s> Live<'s> {
    /// The venue's order entry: [`Verdict::Skipped`] when it declares none, a failure when it
    /// declares one and the setup states no [`OrderEntryStub`].
    pub fn new(
        check: &'static str,
        subject: &'s Subject<'static>,
    ) -> Result<Result<Live<'s>, Verdict>, Failure> {
        let h = Harness::new(check, subject)?;
        let Some(exec) = &h.caps.exec else {
            let why = "VenueCaps.exec is None: the venue takes no orders";
            return Ok(Err(Verdict::Skipped { check, why }));
        };
        let order = exec.order.clone();
        if subject.setup().order_entry.is_none() {
            let what = "the venue declares order entry, yet the setup states no OrderEntryStub \
                        for the stub server to answer it with";
            return Err(h.fail("Setup.order_entry", what));
        }
        Ok(Ok(Live {
            venue: subject.factory(),
            subject,
            h,
            order,
        }))
    }

    /// Runs the venue's order-entry session against the stub: the stub answers the frames the
    /// session writes as its epoch opens with the setup's opening responders, then each request
    /// in turn with the setup's reply, its items answered as the matching entry of `requests`
    /// says. `scenario` drives the session meanwhile; its failure comes back with what the
    /// session and the stub ended with.
    pub fn run<T>(
        &self,
        requests: Vec<Vec<Answer>>,
        scenario: impl AsyncFnOnce(&mut Ctx<'_>) -> Result<T, Failure>,
    ) -> Result<T, Failure> {
        let setup = self.subject.setup();
        let stub = setup.order_entry.expect("Live::new saw an OrderEntryStub");
        let opening = stub.opening.len();
        let mut steps = vec![Step::Accept];
        let respond = |with| Step::Respond { conn: 0, with };
        steps.extend(stub.opening.iter().cloned().map(respond));
        let replies = requests.into_iter().map(|a| stub.reply.responder(a));
        steps.extend(replies.map(respond));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("a current-thread runtime builds outside any other");
        let mut cfg = setup.cfg;
        let creds = setup.creds;
        rt.block_on(async move {
            // A blocked thread keeps the paused clock from moving on its own.
            let (thaw, frozen) = std::sync::mpsc::channel::<()>();
            tokio::task::spawn_blocking(move || frozen.recv());
            let server = StubServer::start(WsScript::new(steps), stub.http).await;
            let server = server.expect("the stub binds two loopback ports");
            (stub.point)(&mut cfg, &server);
            let heard = Heard::default();
            let config = ExecSessionConfig {
                venue: self.venue,
                cfg,
                creds,
                acct: ACCT,
                rpc_ids: RpcIds::default(),
                ns: NAMESPACE,
                specs: self.h.specs().clone(),
                connector: Connector::new(ProxyConfig::Direct),
                pacing: pacing(),
                clock: IngestClock::new(),
                nonces: Box::new(Counting(1)),
                nonce_source: fbc_journal::NonceSourceId(0),
                conn: 0,
                limiter: self.limiter()?,
                write_stall: WriteStall::new(Duration::from_secs(3_600)).expect("a window"),
                http_max_body: 1 << 20,
            };
            let built = ExecSession::new(config, heard.clone());
            let (mut session, control) = built.map_err(|e| {
                self.h
                    .fail("ExecSession::new", format!("refused the venue: {e:?}"))
            })?;
            let mut ctx = Ctx {
                h: &self.h,
                orders: session.orders(),
                heard: heard.clone(),
                oms: Oms::new(&self.h, self.venue.id(), &self.order)?,
                server: &server,
                opening,
                sent: Cell::new(0),
            };
            let script = async {
                let out = scenario(&mut ctx).await;
                drop(control);
                out
            };
            let (ran, out) = tokio::join!(session.run(), script);
            drop(thaw);
            // A session that ended with an error fails the check, whatever the scenario saw
            // (Codex r4222138070).
            let ended = ran.err().map(|e| {
                let what = format!("the session ended with {e:?}");
                Breach::new("ExecSession::run", what)
            });
            let mut failure = match out {
                Ok(seen) => match ended {
                    None => return Ok(seen),
                    Some(breach) => self.h.fail(&breach.capability, breach.what),
                },
                Err(mut failure) => {
                    failure.breaches.extend(ended);
                    failure
                }
            };
            if let Some(Err(e)) = server.finished().now_or_never() {
                let what = format!("the stub's script stopped: {e}");
                failure.breaches.push(Breach::new("OrderEntryStub", what));
            }
            Err(failure)
        })
    }

    /// The venue's declared limits as the session's buckets, none of each kept for safety.
    fn limiter(&self) -> Result<RateLimiter, Failure> {
        let caps: &VenueCaps = &self.h.caps;
        let reserve = SafetyReserve::percent(0).expect("0% is a reserve");
        RateLimiter::new(&caps.limits, reserve)
            .map_err(|e| self.h.fail("VenueCaps.limits", format!("refused: {e:?}")))
    }
}

/// Reconnects paced as a consumer's: 10 ms to 100 ms apart, at most 100 attempts.
fn pacing() -> ReconnectPacing {
    let ms = Duration::from_millis;
    let pacing = ReconnectPacing::new(
        ms(10),
        ms(100),
        100,
        Duration::from_secs(60),
        Duration::from_secs(5),
    );
    pacing.expect("a valid pacing")
}

/// Nonces counted up from the one given.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).expect("nonces far from the end");
        self.0 += u64::from(len);
        block
    }
}

/// What the session told its handler: every event and every submission's handle.
#[derive(Clone, Default)]
pub(crate) struct Heard {
    events: Rc<RefCell<Vec<ExecEvent>>>,
    handles: Rc<RefCell<Vec<SubmitHandle>>>,
}

impl ExecHandler for Heard {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.handles.borrow_mut().push(handle);
    }
}

/// What a scenario drives the session with.
pub(crate) struct Ctx<'a> {
    pub h: &'a Harness<'a>,
    orders: ExecOrders,
    heard: Heard,
    pub oms: Oms,
    server: &'a StubServer,
    /// How many frames the stub answers as the epoch opens, before the first request.
    opening: usize,
    /// How many requests the check has sent.
    sent: Cell<usize>,
}

impl Ctx<'_> {
    /// Lets the session run, without moving the clock, until `done` holds: false when it never
    /// does.
    pub async fn settle(&self, done: impl Fn(&Ctx<'_>) -> bool) -> bool {
        for _ in 0..TURNS {
            if done(self) {
                return true;
            }
            tokio::task::yield_now().await;
        }
        done(self)
    }

    /// Waits until the session's epoch takes places, as it does once the stub has answered
    /// the opening.
    pub async fn ready(&self) -> Result<(), Failure> {
        if self.settle(|c| c.orders.may_place()).await {
            return Ok(());
        }
        let what = "the session's epoch never took places: the opening the stub answered with \
                    did not authenticate it, accept its cancel-on-disconnect arm and end its \
                    resync";
        Err(self.h.fail("OrderEntryStub.opening", what))
    }

    /// Submits `auth`, a request of `op`, to the session and waits until it reports the
    /// request sent: its request id, or a failure when the session refuses it, reports it not
    /// sent, or never reports it. First, where a limit counting `op` allows no more units than
    /// the frames the session has written so far (the opening's and the check's earlier
    /// requests, each counted as one unit whatever it is), the clock moves on by that limit's
    /// window, so its bucket has room again (Codex r4222138060); and only then, so a keepalive
    /// the codec sends meanwhile is not read in a request's place (Codex r4222379993).
    pub async fn send(&self, auth: Authorization, op: OpKind) -> Result<RpcId, Failure> {
        let written = self.opening + self.sent.get();
        let full = |l: &&RateLimit| l.ops.contains(op) && l.units as usize <= written;
        let window = self.h.caps.limits.iter().filter(full).map(|l| l.per).max();
        self.advance(window.unwrap_or_default()).await;
        self.sent.set(self.sent.get() + 1);
        let rpc = self.orders.submit(auth);
        let handle = |c: &Ctx<'_>| {
            let rpc = rpc.as_ref().ok()?;
            let handles = c.heard.handles.borrow();
            handles.iter().find(|h| h.rpc == *rpc).cloned()
        };
        self.settle(|c| rpc.is_err() || handle(c).is_some()).await;
        match (handle(self).map(|h| h.receipt), &rpc) {
            (Some(Ok(_)), Ok(rpc)) => Ok(*rpc),
            // Refused by the session, not sent for the reason given, or never reported.
            (receipt, rpc) => {
                let what = format!("request {rpc:?} was not sent: {receipt:?}");
                Err(self.h.fail("ExecCodec::encode", what))
            }
        }
    }

    /// Waits until the stub has played its script to its end (every request it answers read
    /// and answered), then lets the answers reach the session.
    pub async fn answered(&self) -> Result<(), Failure> {
        let mut finished = None;
        for _ in 0..TURNS {
            finished = self.server.finished().now_or_never();
            if finished.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        match finished {
            Some(Ok(())) => {
                for _ in 0..CHURN {
                    tokio::task::yield_now().await;
                }
                Ok(())
            }
            // The script stopped (a reply refused what it read), or (`None`) still waits for a
            // request the session never wrote.
            other => {
                let what = format!("the stub's script did not play to its end: {other:?}");
                Err(self.h.fail("OrderEntryStub", what))
            }
        }
    }

    /// Lets every task run, without moving the clock, for what the stub's answer brings to
    /// reach the session.
    pub async fn churn(&self) {
        for _ in 0..CHURN {
            tokio::task::yield_now().await;
        }
    }

    /// Moves the clock on, in [`STEP`]s and after [`FINE`] in [`COARSE`] ones, letting the
    /// session run after each, until `done` holds or [`WAIT`] has passed: how far it moved, or
    /// `None` when `done` never held.
    pub async fn advance_until(&self, done: impl Fn(&Ctx<'_>) -> bool) -> Option<Duration> {
        let mut moved = Duration::ZERO;
        while moved < WAIT {
            if done(self) {
                return Some(moved);
            }
            let step = if moved < FINE { STEP } else { COARSE };
            tokio::time::advance(step).await;
            moved += step;
            for _ in 0..CHURN {
                tokio::task::yield_now().await;
            }
        }
        done(self).then_some(moved)
    }

    /// Moves the clock on by `by`, in [`STEP`]s and after [`FINE`] in [`COARSE`] ones, letting
    /// the session run after each.
    pub async fn advance(&self, by: Duration) {
        let mut moved = Duration::ZERO;
        while moved < by {
            let step = if moved < FINE { STEP } else { COARSE };
            tokio::time::advance(step).await;
            moved += step;
            for _ in 0..CHURN {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Every outcome the session reported for request `rpc`, in order.
    pub fn outcomes(&self, rpc: RpcId) -> Vec<(Option<ItemRef>, SubmitOutcome)> {
        let events = self.heard.events.borrow();
        let of = |e: &ExecEvent| match e {
            ExecEvent::Outcome {
                rpc: r,
                item,
                outcome,
            } if *r == rpc => Some((item.clone(), outcome.clone())),
            _ => None,
        };
        events.iter().filter_map(of).collect()
    }

    /// Every event the session reported, in order.
    pub fn events(&self) -> Vec<ExecEvent> {
        self.heard.events.borrow().clone()
    }

    /// How many frames the stub received on the session's first connection that carry `cid` as
    /// the venue's wire spells it (Codex r4222138042: a request rebuilt and signed again is
    /// another frame carrying the same order); where none does, the venue does not send our
    /// id as text, and the frames equal to the first request's (the first frame after the
    /// opening, the one request a check sends that carries `cid`) are counted instead.
    pub fn written(&self, cid: ClientOrderId) -> usize {
        let frames = self.frames();
        let wire = encode_cid(&self.oms.order.client_id, cid).ok();
        let carries = |f: &Frame| {
            let bytes = match f {
                Frame::Text(t) => t.as_bytes(),
                Frame::Binary(b) => b,
            };
            let wire = wire.as_ref().map_or(&[][..], |w| w.as_bytes());
            !wire.is_empty() && bytes.windows(wire.len()).any(|w| w == wire)
        };
        match frames.iter().filter(|f| carries(f)).count() {
            0 => {
                let request = frames.get(self.opening);
                frames.iter().filter(|f| Some(*f) == request).count()
            }
            times => times,
        }
    }

    /// Every frame the stub received, on every connection the session opened, in accept order
    /// (Codex r4222379985: a request written again after a reconnect counts too).
    pub fn frames(&self) -> Vec<Frame> {
        let conns = self.server.connections();
        conns.into_iter().flat_map(|c| c.received).collect()
    }
}

/// An fbc-oms registry armed on the setup's first instrument after a resync that showed the
/// account flat with nothing resting, with the owner's Start, its leases taken in a directory
/// of its own: every order command a check sends is one it built and authorized, through its
/// public path (`Registry::authorize`), as a live consumer's are.
pub(crate) struct Oms {
    reg: Registry,
    order: OrderCaps,
    shape: Shape,
    cids: Vec<ClientOrderId>,
    /// Declared last, so the registry's leases are given up before their directory goes.
    _dir: LeaseDir,
}

/// A directory of the suite's own, removed when dropped.
struct LeaseDir(PathBuf);

impl Drop for LeaseDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

impl Oms {
    /// The registry for `venue`'s account, armed on the harness's instrument.
    fn new(h: &Harness<'_>, venue: &str, order: &OrderCaps) -> Result<Oms, Failure> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let k = NEXT.fetch_add(1, Ordering::Relaxed);
        let name = format!("fbc-conformance-oms-{}-{k}", std::process::id());
        let dir = LeaseDir(std::env::temp_dir().join(&name));
        fs::create_dir_all(&dir.0).expect("a directory of the suite's own");
        let Some(shape) = Shape::plain(order) else {
            let what = "declares no order of a kind, time in force and channel it allows";
            return Err(h.fail("OrderCaps", what));
        };
        let spec = h.specs().get(h.inst).expect("the harness's instrument");
        let symbol = spec.venue_symbol.clone();
        let keys = LeaseKeys::new(venue, &name, order).with_market(h.inst, symbol.clone());
        let cap = h.qty.get().saturating_mul(CAP_ORDERS);
        let cap = Some(Lots::new(cap).expect("a count"));
        let limits = MarketCapsConfig {
            inventory: cap,
            resting: cap,
        };
        let pre_trade = PreTradeCaps::new().with_market(h.inst, limits);
        let pre_trade = pre_trade.expect("positive caps");
        let mut reg = Registry::with_caps(pre_trade).with_lease_keys(keys);
        let snap = ResyncSnapshot {
            watermark: WallNs(1_000),
            requested_at: MonoNs(1_000),
            orders: Vec::new(),
            positions: vec![(h.inst, SignedLots(0))],
        };
        let second = Duration::from_secs(1);
        let ladder = LadderConfig::new(second, Duration::ZERO, 10 * second, 1);
        let ladder = ladder.expect("a valid ladder");
        let key = OrderKey {
            venue: None,
            ingest: 1,
        };
        // A snapshot of the suite's own making: flat, nothing resting, one position.
        let resynced = reg.resync(&ladder, order, &snap, key);
        resynced.expect("a flat resync is taken");
        let market = MarketLease::acquire(&dir.0, venue, &name, &symbol);
        let market = market.expect("a market lease in a directory of the suite's own");
        let acct = AccountLease::acquire(&dir.0, venue, &name);
        let acct = acct.expect("an account lease in a directory of the suite's own");
        let started = reg.start(h.inst, Leases::market(market).with_account(acct));
        let refused = |e| h.fail("fbc-oms", format!("refused the owner's Start: {e:?}"));
        started.map_err(refused)?;
        let cids = h
            .cids(CIDS)
            .expect("ids minted under the suite's own lease");
        Ok(Oms {
            reg,
            order: order.clone(),
            shape,
            cids,
            _dir: dir,
        })
    }

    /// The next of our client ids.
    fn cid(&mut self) -> ClientOrderId {
        // No check places more than three orders.
        self.cids.remove(0)
    }

    /// A buy of the plainest shape the caps allow, at the harness's price and size.
    fn new_order(&mut self, h: &Harness<'_>) -> NewOrder {
        let kind = match self.shape.kind {
            OrderKindTag::Limit => OrderKind::Limit { px: h.px },
            OrderKindTag::Market => OrderKind::Market,
        };
        NewOrder {
            cid: self.cid(),
            inst: h.inst,
            side: Side::Buy,
            kind,
            qty: h.qty,
            tif: self.shape.tif,
            channel: self.shape.channel,
            post_only: self.shape.post_only,
            reduce_only: self.shape.reduce_only,
            reducing: false,
        }
    }

    /// A placement, built and authorized: its client id and authorization.
    pub fn place(&mut self, h: &Harness<'_>) -> Result<(ClientOrderId, Authorization), Failure> {
        let order = self.new_order(h);
        let cid = order.cid;
        let cmd = self.reg.place(order);
        let cmd = cmd.map_err(|e| h.fail("fbc-oms", format!("refused a placement: {e:?}")))?;
        Ok((cid, self.authorize(h, cmd)?))
    }

    /// A batch of `n` placements, built and authorized: their client ids and its authorization.
    pub fn batch(
        &mut self,
        h: &Harness<'_>,
        n: usize,
    ) -> Result<(Vec<ClientOrderId>, Authorization), Failure> {
        let orders: Vec<_> = (0..n).map(|_| self.new_order(h)).collect();
        let cids = orders.iter().map(|o| o.cid).collect();
        let plan = self.reg.place_batch(orders).expect("a batch of one market");
        let cmd = plan.command.filter(|_| plan.refused.is_empty());
        let refused = || h.fail("fbc-oms", format!("refused items: {:?}", plan.refused));
        Ok((cids, self.authorize(h, cmd.ok_or_else(refused)?)?))
    }

    /// The session's `events` applied in the order reported, as a live consumer applies them:
    /// each outcome of request `rpc`, order `cid`'s placement, and each order update
    /// (Codex r4222779025). An outcome for the whole request is its one item's (Codex
    /// r4222379990).
    pub fn replay(
        &mut self,
        h: &Harness<'_>,
        cid: ClientOrderId,
        rpc: RpcId,
        events: &[ExecEvent],
    ) -> Result<(), Failure> {
        let whole = ItemRef {
            idx: 0,
            cid: None,
            vid: None,
        };
        for (ingest, event) in (0..).zip(events) {
            match event {
                ExecEvent::Outcome {
                    rpc: r,
                    item,
                    outcome,
                } if *r == rpc => {
                    let item = item.as_ref().unwrap_or(&whole);
                    let now = MonoNs(2_000);
                    let applied = self.reg.on_outcome(cid, OrderOp::Place, item, outcome, now);
                    let refused =
                        |e| h.fail("fbc-oms", format!("refused the placement's outcome: {e:?}"));
                    applied.map_err(refused)?;
                }
                ExecEvent::Order(u) => {
                    let key = OrderKey {
                        venue: None,
                        ingest,
                    };
                    self.reg.apply_update(u, key);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// The venue id fbc-oms knows order `cid` by, if any.
    pub fn vid(&self, cid: ClientOrderId) -> Option<VenueOrderId> {
        self.reg.get(cid).and_then(|o| o.vid().cloned())
    }

    /// An amend of resting order `cid` to `px` and `qty`, built and authorized.
    pub fn amend(
        &mut self,
        h: &Harness<'_>,
        cid: ClientOrderId,
        px: Ticks,
        qty: Lots,
    ) -> Result<Authorization, Failure> {
        let live = self.reg.live(cid);
        let live = live.map_err(|e| h.fail("fbc-oms", format!("no live order: {e:?}")))?;
        let cmd = live.amend(&self.order, px, qty, false);
        let cmd = cmd.map_err(|e| h.fail("fbc-oms", format!("refused the amend: {e:?}")))?;
        self.authorize(h, cmd)
    }

    fn authorize(
        &self,
        h: &Harness<'_>,
        cmd: fbc_oms::PermittedCommand,
    ) -> Result<Authorization, Failure> {
        let auth = self.reg.authorize(ACCT, cmd);
        auth.map_err(|e| h.fail("fbc-oms", format!("refused to authorize: {e:?}")))
    }
}
