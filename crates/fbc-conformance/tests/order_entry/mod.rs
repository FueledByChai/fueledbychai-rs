//! What the order-entry fault scripts share (FBC-6to, and the scripts after it): fbc-runtime's
//! order-entry session for the conformance toy, built from its factory, run against the stub
//! server playing a script of typed steps (decision 0025) whose replies are computed from the
//! requests it reads (decision 0047), on a paused clock, with every order command built and
//! authorized by an fbc-oms registry and every event the session reports applied to that
//! registry as a live consumer applies it ([`Glue`]).
//!
//! The opening is answered as the toy's own setup answers it: the authentication acknowledged,
//! the cancel-on-disconnect arm accepted, and a resync showing the account flat on
//! [`INST_A`] with nothing resting, which the glue applies to the registry before the owner's
//! Start. A script's own steps follow; each reply a test waits on ends in a
//! [`Step::Barrier`], whose pong says the session has handed every frame before it to the
//! glue, so a test waits on events, never on a count of scheduler turns (FBC-pn85).

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fbc_conformance::toy::{self, EXEC_URL_KEY, INST_A, OWN_NS, SYMBOL_A, TOY_TOKEN, ToyFactory};
use fbc_conformance::{Frame, HttpRouter, Responded, Responder, Step, StubServer, WsScript};
use fbc_core::{
    AccountKey, AccountLease, Channel, CidMint, ClientOrderId, ConnState, Envelope, ExecEvent,
    ItemRef, Lots, MarketLease, MonoNs, NamespaceLease, NewOrder, NonceBlock, NonceSource,
    OrderCaps, OrderKind, RpcId, Secrets, Side, SubmitHandle, Ticks, Tif, VenueConfig,
    VenueFactory, WallNs,
};
use fbc_oms::{
    Admission, Applied, CancelChoice, FillApplied, FillLedger, FillRouted, FillTime, LadderConfig,
    LeaseKeys, Leases, LedgerConfig, MarketCapsConfig, OrderKey, OrderOp, PreTradeCaps, Registry,
    ResyncSnapshot, Routed,
};
use fbc_runtime::{
    Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, IngestClock, ProxyConfig,
    RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};

/// The account the session trades and fbc-oms authorizes for.
pub const ACCT: AccountKey = AccountKey::new(1);
/// The conformance toy, as its factory builds it for the session.
pub static TOY: ToyFactory = ToyFactory;
/// The registry's inventory and resting caps on [`INST_A`], in lots: room for every order here.
const CAP: i64 = 1_000;
/// How long, by the wall clock, a wait lasts before the test fails: far longer than a loaded
/// host takes, so only a script the session never answers fails it.
const WATCHDOG: Duration = Duration::from_secs(60);

/// The value of `key` in a toy record `kind|key=value|...` (the first line of a request).
pub fn field(frame: &Frame, key: &str) -> Result<String, String> {
    let text = text(frame)?;
    let head = text.lines().next().unwrap_or_default();
    let found = head
        .split('|')
        .skip(1)
        .filter_map(|kv| kv.split_once('='))
        .find_map(|(k, v)| (k == key).then(|| v.to_owned()));
    found.ok_or_else(|| format!("no {key} in {head}"))
}

/// A responder answering a request of `kind` with the records `with` computes from it.
pub fn answer(
    kind: &'static str,
    with: impl Fn(&Frame) -> Result<Vec<String>, String> + Send + Sync + 'static,
) -> Responder {
    Responder::new(move |frame| -> Responded {
        let read = text(frame)?.split('|').next().unwrap_or_default();
        let named = (read == kind).then_some(());
        named.ok_or_else(|| format!("expected {kind}, read {read}"))?;
        Ok(with(frame)?.into_iter().map(Frame::Text).collect())
    })
}

/// The text of a text frame; the toy sends and reads no other.
fn text(frame: &Frame) -> Result<&str, String> {
    match frame {
        Frame::Text(text) => Ok(text),
        Frame::Binary(_) => Err("a binary frame".into()),
    }
}

/// The opening, answered as the toy's setup answers it, the resync showing the account flat on
/// [`INST_A`] with nothing resting; then a barrier, so the glue has applied the resync once it
/// has passed.
fn opening() -> Vec<Step> {
    let respond = |with| Step::Respond { conn: 0, with };
    vec![
        Step::Accept,
        respond(answer("auth", |_| {
            Ok(vec![format!("auth|ok=1|token={TOY_TOKEN}")])
        })),
        respond(answer("cod", |r| {
            Ok(vec![format!("item|rpc={}|i=0|res=ok", field(r, "rpc")?)])
        })),
        respond(answer("resync", |r| {
            Ok(vec![
                format!("rsbegin|wm={}", field(r, "ts")?),
                format!("rspos|sym={SYMBOL_A}|qty=0"),
                "rsend".into(),
            ])
        })),
        Step::Barrier { conn: 0 },
    ]
}

/// A script's steps after the opening: each reply it is given is answered from the request it
/// reads and followed by a barrier, whose position a test waits on.
#[derive(Default)]
pub struct Script {
    steps: Vec<Step>,
    /// How many of its steps have played once each barrier has passed, in order.
    barriers: Vec<usize>,
}

impl Script {
    /// The script, its next request answered by `with`.
    pub fn reply(mut self, with: Responder) -> Script {
        self.steps.push(Step::Respond { conn: 0, with });
        self.steps.push(Step::Barrier { conn: 0 });
        self.barriers.push(self.steps.len());
        self
    }
}

/// A buy of 25 lots of [`INST_A`], post-only and good till cancelled, as the toy's caps allow.
pub fn buy(cid: ClientOrderId) -> NewOrder {
    NewOrder {
        cid,
        inst: INST_A,
        side: Side::Buy,
        kind: OrderKind::Limit { px: Ticks(130_865) },
        qty: Lots::new(25).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// Where an event the glue applied went.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Seen {
    /// An order update, as the registry routed it.
    Order(Routed),
    /// A fill the ledger accepted, as the registry routed it.
    Fill(FillRouted),
    /// A fill the ledger held already.
    Duplicate,
}

/// The registry, the fill ledger and what was submitted, which every event the session reports
/// is applied to, as `fbc-examples`' glue applies them for a live consumer: an outcome to the
/// order its request named, an order update under the venue's sequence with the ingest order as
/// tiebreak, a fill through the ledger and, accepted, to the registry, and the resync's events
/// collected into one snapshot applied at its end.
pub struct State {
    pub reg: Registry,
    ledger: FillLedger,
    caps: OrderCaps,
    ladder: LadderConfig,
    /// The order and operation of each request submitted here.
    sent: HashMap<RpcId, (ClientOrderId, OrderOp)>,
    collecting: Option<ResyncSnapshot>,
    authenticated_at: Option<MonoNs>,
    /// When the last event arrived, on the session's monotonic clock: a submission's send time.
    last: MonoNs,
    /// How many resyncs the registry applied.
    pub resynced: usize,
    /// Every order update and fill, where it went, in the order reported.
    pub seen: Vec<Seen>,
}

impl State {
    /// Applies one event the session reported; the test fails on anything the registry or the
    /// ledger refuses, which a live consumer would have to treat as trouble.
    fn on_event(&mut self, env: Envelope<ExecEvent>) {
        let now = env.stamp.recv_mono;
        self.last = now;
        let key = OrderKey {
            venue: env.venue_seq,
            ingest: env.stamp.ingest_seq,
        };
        match env.body {
            ExecEvent::Conn {
                state: ConnState::Authenticated,
                ..
            } => self.authenticated_at = Some(now),
            ExecEvent::ResyncBegin { watermark } => {
                self.collecting = Some(ResyncSnapshot {
                    watermark,
                    requested_at: self
                        .authenticated_at
                        .expect("authenticated before a resync"),
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
                let resynced = self.reg.resync(&self.ladder, &self.caps, &snap, key);
                resynced.expect("the registry takes the resync");
                self.resynced += 1;
            }
            // The session's own requests (its cancel-on-disconnect arm) name no order here.
            ExecEvent::Outcome { rpc, item, outcome } if self.sent.contains_key(&rpc) => {
                let (cid, op) = self.sent[&rpc];
                let whole = ItemRef {
                    idx: 0,
                    cid: Some(cid),
                    vid: None,
                };
                let item = item.unwrap_or(whole);
                let applied = self.reg.on_outcome(cid, op, &item, &outcome, now);
                applied.expect("the registry takes the outcome");
            }
            ExecEvent::Order(u) => {
                let routed = self.reg.apply_update(&u, key);
                self.seen.push(Seen::Order(routed));
            }
            ExecEvent::Fill(f) => {
                let time = env.exch_ts.map(|exch| FillTime {
                    exch,
                    kind: env.exch_ts_kind,
                    aligned: WallNs(exch.0),
                });
                let seen = match self.ledger.admit(&f, time, now) {
                    Admission::Apply(accepted) => Seen::Fill(
                        self.reg
                            .apply_fill(accepted)
                            .expect("the registry counts it"),
                    ),
                    Admission::Duplicate => Seen::Duplicate,
                    other => panic!("a live fill not admitted: {other:?}"),
                };
                self.seen.push(seen);
            }
            _ => {}
        }
    }

    /// The resync being collected.
    fn collecting(&mut self) -> &mut ResyncSnapshot {
        self.collecting.as_mut().expect("a resync begun")
    }

    /// Records a submission sent: a placement's send time, a cancel in flight. Every one here
    /// is sent, since no deadline passes and the toy's buckets have room.
    fn on_submitted(&mut self, handle: SubmitHandle) {
        let (cid, op) = self.sent[&handle.rpc];
        handle.receipt.expect("sent");
        let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let wall = WallNs(i64::try_from(wall.as_nanos()).unwrap());
        let recorded = match op {
            OrderOp::Place => self.reg.placement_sent(cid, self.last, wall),
            OrderOp::Cancel(rpc) => self.reg.cancel_sent(cid, rpc, self.last).map(drop),
            OrderOp::Amend(_) => unreachable!("no script here amends"),
        };
        recorded.expect("the registry records it sent");
    }
}

/// The session's handler: the shared [`State`].
#[derive(Clone)]
pub struct Glue(pub Rc<RefCell<State>>);

impl ExecHandler for Glue {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.0.borrow_mut().on_event(env);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.0.borrow_mut().on_submitted(handle);
    }
}

/// A directory of the test's own for its leases, removed when dropped.
pub struct LeaseDir(pub PathBuf);

impl LeaseDir {
    pub fn new() -> LeaseDir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let k = NEXT.fetch_add(1, Ordering::Relaxed);
        let name = format!("fbc-conformance-faults-{}-{k}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        fs::create_dir_all(&dir).unwrap();
        LeaseDir(dir)
    }

    pub fn account(&self) -> String {
        self.0.file_name().unwrap().to_string_lossy().into_owned()
    }
}

impl Drop for LeaseDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Nonces counted up from 1.
pub struct Counting(pub u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

/// What a script's scenario drives the session with.
pub struct Run<'a> {
    pub st: Rc<RefCell<State>>,
    orders: ExecOrders,
    server: &'a StubServer,
    barriers: Vec<usize>,
    /// How many of the script's barriers the scenario has waited for.
    waited: usize,
    mint: CidMint,
    dir: &'a LeaseDir,
}

impl Run<'_> {
    /// Lets the session and the stub run, without moving the clock, until `done` holds; the test
    /// fails, naming `what`, once the watchdog's wall-clock time has passed.
    async fn until(&self, what: &str, done: impl Fn(&Run<'_>) -> bool) {
        let give_up = Instant::now() + WATCHDOG;
        while !done(self) {
            assert!(Instant::now() < give_up, "never {what}");
            tokio::task::yield_now().await;
        }
    }

    /// Waits until the session's epoch takes places and the glue has applied its resync, then
    /// gives the owner's Start on [`INST_A`].
    pub async fn started(&mut self) {
        self.until("ready", |r| {
            r.orders.may_place() && r.st.borrow().resynced == 1
        })
        .await;
        let account = self.dir.account();
        let symbol = toy::specs().get(INST_A).unwrap().venue_symbol.clone();
        let market = MarketLease::acquire(&self.dir.0, TOY.id(), &account, &symbol).unwrap();
        let acct = AccountLease::acquire(&self.dir.0, TOY.id(), &account).unwrap();
        let leases = Leases::market(market).with_account(acct);
        self.st.borrow_mut().reg.start(INST_A, leases).unwrap();
    }

    /// Places a buy ([`buy`]), built and authorized by the registry and submitted to the
    /// session: its client id and request id.
    pub fn place(&mut self) -> (ClientOrderId, RpcId) {
        let cid = self.mint.mint().unwrap();
        let mut st = self.st.borrow_mut();
        let cmd = st.reg.place(buy(cid)).unwrap();
        let auth = st.reg.authorize(ACCT, cmd).unwrap();
        let rpc = self.orders.submit(auth).unwrap();
        st.sent.insert(rpc, (cid, OrderOp::Place));
        (cid, rpc)
    }

    /// Cancels order `cid` through its cancel permit, authorized and submitted: its request id.
    pub fn cancel(&mut self, cid: ClientOrderId) -> RpcId {
        let mut st = self.st.borrow_mut();
        let caps = st.caps.clone();
        let permit = st.reg.cancellable(cid).unwrap();
        let CancelChoice::Send(cmd) = permit.cancel(&caps) else {
            panic!("the toy cancels before its acknowledgement, by our client id");
        };
        let auth = st.reg.authorize(ACCT, cmd).unwrap();
        let rpc = self.orders.submit(auth).unwrap();
        st.sent.insert(rpc, (cid, OrderOp::Cancel(rpc)));
        rpc
    }

    /// Waits until the stub has read the next request the script answers and the glue has
    /// applied every frame of its reply: the barrier after it has passed.
    pub async fn replied(&mut self) {
        let at = self.barriers[self.waited];
        self.waited += 1;
        self.until("replied", |r| r.server.played() >= at).await;
    }
}

/// What a run of a script left: the registry and the glue's record, and every frame the stub
/// received.
pub struct Played<T> {
    pub out: T,
    pub st: State,
    pub received: Vec<Frame>,
}

/// Plays `script` after the opening against fbc-runtime's order-entry session for the toy,
/// with `scenario` driving it, on a paused clock nothing moves: no deadline passes. The run
/// ends once the scenario has waited for every reply and the script has played to its end.
pub fn play<T>(script: Script, scenario: impl AsyncFnOnce(&mut Run<'_>) -> T) -> Played<T> {
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
    let second = Duration::from_secs(1);
    let ledger = LedgerConfig {
        max_age: 3_600 * second,
        max_entries: 1_024,
    };
    let st = Rc::new(RefCell::new(State {
        reg,
        // Live fills only: the watermark is read by replayed fills alone.
        ledger: FillLedger::new(ledger, WallNs(0)).unwrap(),
        caps: order,
        ladder: LadderConfig::new(second, Duration::ZERO, 10 * second, 1).unwrap(),
        sent: HashMap::new(),
        collecting: None,
        authenticated_at: None,
        last: MonoNs(0),
        resynced: 0,
        seen: Vec::new(),
    }));
    let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
    let mint = CidMint::new(lease, 0, 0, WallNs(1));
    let mut steps = opening();
    let opened = steps.len();
    steps.extend(script.steps);
    let end = steps.len();
    let barriers = script.barriers.iter().map(|at| opened + at).collect();
    let out = rt.block_on(async {
        // A blocked thread keeps the paused clock from moving on its own.
        let (thaw, frozen) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || frozen.recv());
        let server = StubServer::start(WsScript::new(steps), HttpRouter::new())
            .await
            .unwrap();
        let mut cfg = VenueConfig::new();
        cfg.insert(EXEC_URL_KEY, &server.ws_url("/exec"));
        let pacing = ReconnectPacing::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap();
        let config = ExecSessionConfig {
            venue: &TOY,
            cfg,
            creds: Secrets::new(),
            acct: ACCT,
            rpc_ids: RpcIds::default(),
            ns: OWN_NS,
            specs: toy::specs(),
            connector: Connector::new(ProxyConfig::Direct),
            pacing,
            clock: IngestClock::new(),
            nonces: Box::new(Counting(1)),
            nonce_source: fbc_journal::NonceSourceId(0),
            conn: 0,
            limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(0).unwrap()).unwrap(),
            write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
            http_max_body: 1 << 20,
        };
        let (mut session, control) = ExecSession::new(config, Glue(st.clone())).unwrap();
        let mut run = Run {
            st: st.clone(),
            orders: session.orders(),
            server: &server,
            barriers,
            waited: 0,
            mint,
            dir: &dir,
        };
        let driven = async {
            let out = scenario(&mut run).await;
            run.until("played to its end", |r| r.server.played() >= end)
                .await;
            drop(control);
            out
        };
        let (ran, out) = tokio::join!(session.run(), driven);
        drop(thaw);
        ran.unwrap();
        server.finished().await.unwrap();
        let received = server.connections().into_iter().flat_map(|c| c.received);
        (out, received.collect())
    });
    drop(rt);
    let st = Rc::into_inner(st).unwrap().into_inner();
    Played {
        out: out.0,
        st,
        received: out.1,
    }
}

/// What a run leaves of order `cid`: its state, venue and fill counts, what rests of it, the
/// inventory on [`INST_A`] and the buys resting there, all of which two runs of the same
/// executions in another order must agree on (decision 0005, I1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    pub state: fbc_oms::OrdState,
    pub filled: Lots,
    pub cum_venue: Lots,
    pub cum_fills: Lots,
    pub resting: Lots,
    pub inventory: fbc_core::SignedLots,
    pub resting_buys: Option<Lots>,
}

impl Outcome {
    pub fn of(reg: &Registry, cid: ClientOrderId) -> Outcome {
        let rec = reg.get(cid).unwrap();
        Outcome {
            state: rec.state(),
            filled: rec.filled(),
            cum_venue: rec.cum_venue(),
            cum_fills: rec.cum_fills(),
            resting: rec.resting(),
            inventory: reg.inventory(INST_A),
            resting_buys: reg.resting_on(INST_A, Side::Buy),
        }
    }
}

/// Whether every event of `seen` is an update of order `cid` ignored as late.
pub fn ignored_late(seen: &[Seen], cid: ClientOrderId) -> bool {
    seen.iter()
        .all(|s| *s == Seen::Order(Routed::Ours(cid, Applied::IgnoredLate)))
}

/// Whether `seen` is a fill counted on `cid` as `applied`.
pub fn counted(seen: &Seen, cid: ClientOrderId, applied: FillApplied) -> bool {
    *seen == Seen::Fill(FillRouted::Ours(cid, applied))
}
