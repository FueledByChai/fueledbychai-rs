//! The sweep of a market's orders of ours (FBC-132g, decision 0155 items 2, 5 and 8), and the
//! link's `live_on`: the conformance toy's order entry on an fbc-runtime `ExecSession` against
//! the conformance stub's serving script, a toy venue of the test's own, on a paused runtime whose
//! clock moves only when the test moves it, every command through an fbc-oms authorization and the
//! session's `ExecOrders`. It shows fbc-oms's instrument cancel-all, or explicit cancels once a
//! cancel not ours was noted or when fbc-oms builds no cancel-all, and never an account
//! cancel-all; the cancel of everything sent again after a refusal for its rate, at the consumer's
//! wait or at the venue's retry time and not before; before places, `AlreadyTerminal` waiting for
//! its order's end, a refusal for its rate waiting for the resend, any other refusal failing the
//! sweep, and no end before the market takes places; an order a `NotFound` puts on fbc-oms's
//! Unknown ladder keeping it waiting; at Stop, an end while the market takes no place, once every
//! request is answered, whatever the answers; the cancels due on an acknowledgement; a cancel not
//! authorized, not submitted or not sent; and the consumer's rate hold. Nothing but the notes,
//! handed over with the link, tells the sweep which requests are cancels on its market
//! (`Link::is_cancel_on`): a cancel of the consumer's refused for its rate after its order ended,
//! the link's Safety cancel so refused and a consumer's instrument cancel-all of the market so
//! refused make the cancel of everything due again, and a place so refused does not, nor does a
//! cancel or a cancel-all on another market. A sweep sends only its own market's cancels due on an
//! acknowledgement, and at Stop another market's cancel left unanswered does not hold it. Before
//! places, a refusal of a cancel not the sweep's fails nothing. The explicit cancels keep the
//! order the link first learnt our orders in through a later resync showing them in another, name
//! again an order whose cancel came back Unknown, and leave one whose cancel awaits its
//! acknowledgement to the cancels due.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use fbc_conformance::toy::{
    EXEC_URL_KEY, INST_A, INST_B, OWN_NS, RPC_TIMEOUT, SYMBOL_A, SYMBOL_B, TOY_TOKEN, ToyFactory,
};
use fbc_conformance::{Frame, HttpRouter, Pusher, Responded, Responder, StubServer, WsScript};
use fbc_core::{
    AccountKey, AccountLease, Channel, CidMatch, CidMint, ClientOrderId, InstrumentId, Lots,
    MarketLease, MonoNs, NamespaceLease, NewOrder, NonceBlock, NonceSource, NotSentReason,
    OrderCaps, OrderKind, RejectKind, RpcId, Secrets, Side, SubmitOutcome, Ticks, Tif, VenueConfig,
    VenueFactory, WallNs, dispatch_market_data, encode_cid,
};
use fbc_live::link::{Link, LinkHandler, Note};
use fbc_live::sweep::{
    How, Polled, RateHold, Round, SendError, Sweep, SweepEnd, SweepState, Why, ZeroWait,
};
use fbc_oms::{
    CancelAllRefusal, CancelChoice, CancelEverything, FillLedger, Intent, LadderConfig, LeaseKeys,
    Leases, LedgerConfig, MarketCapsConfig, OrdState, PermittedCommand, PreTradeCaps, Registry,
};
use fbc_runtime::{
    Connector, ExecControl, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, InFlight,
    IngestClock, ProxyConfig, RateLimiter, ReconnectPacing, ReservedBound, ResyncDeadline, RpcIds,
    SafetyReserve, WriteStall,
};

static TOY: ToyFactory = ToyFactory;

/// The account the session trades and the registry authorizes for.
const ACCT: AccountKey = AccountKey::new(1);

/// How long, by the wall clock, a wait lasts before the test fails: far longer than a loaded
/// host takes, so only a stub the session never answers fails it.
const WATCHDOG: Duration = Duration::from_secs(60);

/// The consumer's wait for a refusal for its rate whose venue states no retry time, as the toy's
/// refusal (code 1006) states none.
const WAIT: Duration = Duration::from_secs(2);

/// The toy's order caps.
fn order_caps() -> OrderCaps {
    fbc_conformance::toy::caps().exec.unwrap().order
}

/// The toy's order caps with no batch cancel: [`Registry::cancel_many`] builds a single cancel
/// of each order.
fn single_caps() -> OrderCaps {
    OrderCaps {
        batch_cancel: None,
        ..order_caps()
    }
}

/// The toy's order caps with no cancel before an order's acknowledgement, and a batch cancel
/// naming an order by the venue's id alone: a cancel of an order the venue has not acknowledged
/// waits for it, and is due once it lands. The toy's batch cancel also names the placement
/// nonce, which fbc-oms takes before the acknowledgement (design §4.9) and the link records
/// (FBC-o0f0), so under the toy's own batch references a sweep names such an order by it at
/// once. A single cancel tries our client id before the nonce, so it waits whatever a batch
/// names.
fn awaiting_caps() -> OrderCaps {
    let caps = order_caps();
    OrderCaps {
        cancel_before_ack: false,
        batch_cancel: caps.batch_cancel.map(|batch| fbc_core::CancelBatch {
            refs: fbc_core::TagSet::of(&[fbc_core::RefKind::Venue]),
            ..batch
        }),
        ..caps
    }
}

/// A directory of the test's own for its leases, removed when it is dropped.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Dir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("fbc-live-sweep-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Nonces counted up from 1.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

/// The value of `key` in the toy record `line` (`kind|key=value|...`).
fn field(line: &str, key: &str) -> String {
    let mut fields = line.split('|').skip(1).filter_map(|kv| kv.split_once('='));
    let found = fields.find_map(|(k, v)| (k == key).then(|| v.to_owned()));
    found.unwrap_or_else(|| panic!("no {key} in {line}"))
}

/// How the test's venue answers a cancel-all, or one cancel (each item of a batch alike).
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// Accepted, then each order it names (a cancel-all: each order of the market resting)
    /// ended `canceled` by the request.
    Cancel,
    /// Refused with the toy's reject `code`, the order left as it was.
    Refuse(&'static str),
    /// Nothing: the test pushes what it wants later.
    Silent,
    /// Each order it names ended, the answer itself held back for the test to push.
    EndOnly,
}

/// An order the test's venue holds: what its place said, and whether it rests.
struct Order {
    cid: String,
    vid: String,
    /// Its market's venue symbol.
    sym: String,
    side: String,
    px: String,
    qty: String,
    /// The place's request id.
    rpc: String,
    resting: bool,
}

impl Order {
    /// The order the place record `line` of request `rpc` asks for, as venue order `vid`, on
    /// the market it names (`SYMBOL_A` when it names none).
    fn placed(line: &str, vid: String, rpc: String, resting: bool) -> Order {
        let named = line.split('|').any(|kv| kv.starts_with("sym="));
        Order {
            cid: field(line, "cid"),
            vid,
            sym: if named {
                field(line, "sym")
            } else {
                SYMBOL_A.to_owned()
            },
            side: field(line, "side"),
            px: field(line, "px"),
            qty: field(line, "qty"),
            rpc,
            resting,
        }
    }
}

/// The test's toy venue, shared with the stub's responder.
#[derive(Default)]
struct Venue {
    seq: u64,
    orders: Vec<Order>,
    /// How each cancel-all, and each cancel item, is answered, in order; once used up, `Cancel`.
    cancel_alls: VecDeque<Answer>,
    cancels: VecDeque<Answer>,
    /// Places are acknowledged only when the test pushes it ([`Run::ack`]).
    hold_acks: bool,
    /// The resync is answered only when the test pushes it ([`Run::resync`]): its watermark.
    hold_resync: bool,
    resync_ts: Option<String>,
    /// Each request's kind as read, in order.
    read: Vec<String>,
    /// The market each cancel-all named.
    cancel_all_syms: Vec<String>,
    /// The session's bound on the commands waiting for its next turn, Normal and reserve; 64
    /// and 64 when unset.
    outbound: Option<(u32, u32)>,
    /// The resync shows the resting orders last placed first.
    resync_reversed: bool,
    /// The registry trades `INST_B` beside `INST_A`, and the resync shows a flat position on
    /// both.
    two_markets: bool,
}

type Shared = Arc<Mutex<Venue>>;

fn lock(venue: &Shared) -> MutexGuard<'_, Venue> {
    venue.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Venue {
    /// Order `idx` ended `state`: its order event.
    fn end(&mut self, idx: usize, state: &str) -> String {
        self.seq += 1;
        let o = &mut self.orders[idx];
        o.resting = false;
        format!(
            "order|cid={}|vid={}|sym={}|side={}|st={state}|cum=0|qty={}|po=1|ro=0|seq={}",
            o.cid, o.vid, o.sym, o.side, o.qty, self.seq
        )
    }

    /// The order a cancel's record `line` names, by its venue id or its client id.
    fn named(&self, line: &str) -> Result<usize, String> {
        let pairs: Vec<(&str, &str)> = line
            .split('|')
            .skip(1)
            .filter_map(|kv| kv.split_once('='))
            .collect();
        let by = |o: &Order| {
            pairs
                .iter()
                .any(|(k, v)| (*k == "vid" && *v == o.vid) || (*k == "cid" && *v == o.cid))
        };
        self.orders
            .iter()
            .position(by)
            .ok_or_else(|| format!("no order named in {line}"))
    }

    fn next_cancel(&mut self) -> Answer {
        self.cancels.pop_front().unwrap_or(Answer::Cancel)
    }

    /// The answers to request `rpc`'s items, each `(how, the orders it names)`, then the order
    /// events of the orders they end.
    fn answer(&mut self, rpc: &str, items: &[(Answer, Vec<usize>)]) -> Vec<String> {
        let (mut frames, mut ends) = (Vec::new(), Vec::new());
        for (i, (answer, orders)) in items.iter().enumerate() {
            match answer {
                Answer::Cancel => {
                    frames.push(format!("item|rpc={rpc}|i={i}|res=ok"));
                    ends.extend(orders);
                }
                Answer::Refuse(code) => {
                    frames.push(format!("item|rpc={rpc}|i={i}|res=rej|code={code}"));
                }
                Answer::Silent => {}
                Answer::EndOnly => ends.extend(orders),
            }
        }
        for idx in ends {
            frames.push(self.end(idx, "canceled|why=requested"));
        }
        frames
    }
}

/// A resync of a flat account at watermark `ts`, showing each order of `orders` that rests, in
/// that order, and a flat position on `SYMBOL_A`, and on `SYMBOL_B` too when `two` holds.
fn resync<'a>(ts: &str, orders: impl IntoIterator<Item = &'a Order>, two: bool) -> Vec<String> {
    let mut records = vec![format!("rsbegin|wm={ts}")];
    for o in orders.into_iter().filter(|o| o.resting) {
        records.push(format!(
            "rsorder|cid={}|vid={}|sym={}|side={}|st=open|px={}|qty={}|cum=0|po=1|ro=0",
            o.cid, o.vid, o.sym, o.side, o.px, o.qty
        ));
    }
    records.push(format!("rspos|sym={SYMBOL_A}|qty=0"));
    if two {
        records.push(format!("rspos|sym={SYMBOL_B}|qty=0"));
    }
    records.push("rsend".to_owned());
    records
}

/// The test's venue's answer to `frame`.
fn respond(venue: &Shared, frame: &Frame) -> Responded {
    let Frame::Text(text) = frame else {
        return Err("the toy sends text frames only".to_owned());
    };
    let mut lines = text.lines();
    let head = lines.next().unwrap_or_default();
    let kind = head.split('|').next().unwrap_or_default();
    let mut v = lock(venue);
    v.read.push(kind.to_owned());
    let frames = match kind {
        "auth" => vec![format!("auth|ok=1|token={TOY_TOKEN}")],
        "cod" => vec![format!("item|rpc={}|i=0|res=ok", field(head, "rpc"))],
        "resync" if v.hold_resync => {
            v.resync_ts = Some(field(head, "ts"));
            Vec::new()
        }
        "resync" if v.resync_reversed => {
            resync(&field(head, "ts"), v.orders.iter().rev(), v.two_markets)
        }
        "resync" => resync(&field(head, "ts"), &v.orders, v.two_markets),
        "place" => {
            let (rpc, vid) = (field(head, "rpc"), format!("V-{}", v.orders.len() + 1));
            let resting = !v.hold_acks;
            v.orders
                .push(Order::placed(head, vid.clone(), rpc.clone(), resting));
            let ack = format!("item|rpc={rpc}|i=0|res=ok|vid={vid}");
            if resting { vec![ack] } else { Vec::new() }
        }
        // Each place of a batch acknowledged.
        "batch" => {
            let rpc = field(head, "rpc");
            let mut acks = Vec::new();
            for (i, line) in lines.enumerate() {
                let vid = format!("V-{}", v.orders.len() + 1);
                v.orders
                    .push(Order::placed(line, vid.clone(), rpc.clone(), true));
                acks.push(format!("item|rpc={rpc}|i={i}|res=ok|vid={vid}"));
            }
            acks
        }
        // An amend is left unanswered.
        "amend" => Vec::new(),
        "cancelall" => {
            let sym = field(head, "sym");
            v.cancel_all_syms.push(sym.clone());
            let answer = v.cancel_alls.pop_front().unwrap_or(Answer::Cancel);
            let on = |o: &Order| o.resting && o.sym == sym;
            let resting = (0..v.orders.len()).filter(|i| on(&v.orders[*i]));
            let item = (answer, resting.collect());
            v.answer(&field(head, "rpc"), &[item])
        }
        "cancel" => {
            let item = (v.next_cancel(), vec![v.named(head)?]);
            v.answer(&field(head, "rpc"), &[item])
        }
        "cancels" => {
            let mut items = Vec::new();
            for line in lines {
                items.push((v.next_cancel(), vec![v.named(line)?]));
            }
            v.answer(&field(head, "rpc"), &items)
        }
        "ping" => Vec::new(),
        other => return Err(format!("the test's venue reads no {other}")),
    };
    Ok(frames.into_iter().map(Frame::Text).collect())
}

/// The toy's order entry for `ACCT` at `url`, stamping on `clock`, handing its events to
/// `handler`: one connection attempt, and deadlines no test here reaches.
fn session<H: ExecHandler>(
    url: &str,
    clock: &IngestClock,
    handler: H,
) -> (ExecSession<H>, ExecControl) {
    session_bounded(url, clock, handler, (64, 64))
}

/// [`session`], the commands waiting for its next turn bounded by `outbound`.
fn session_bounded<H: ExecHandler>(
    url: &str,
    clock: &IngestClock,
    handler: H,
    outbound: (u32, u32),
) -> (ExecSession<H>, ExecControl) {
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, url);
    let limits = TOY.caps(&cfg).unwrap().limits;
    let ms = Duration::from_millis;
    let config = ExecSessionConfig {
        venue: &TOY,
        cfg,
        creds: Secrets::new(),
        acct: ACCT,
        account_wide_cod_authority: None,
        rpc_ids: RpcIds::default(),
        ns: OWN_NS,
        specs: fbc_conformance::toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 1, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: clock.clone(),
        nonces: Box::new(Counting(1)),
        nonce_source: fbc_journal::NonceSourceId(1),
        conn: 0,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        resync_deadline: ResyncDeadline::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 1 << 20,
        outbound: ReservedBound::new(outbound.0, outbound.1).unwrap(),
        in_flight: InFlight::new(ReservedBound::new(64, 64).unwrap()),
    };
    ExecSession::new(config, handler).unwrap()
}

/// A link over `reg` under `caps`.
fn link_over(reg: Registry, caps: OrderCaps) -> Link {
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
    Link::new(reg, ledger, caps, ladder)
}

/// A registry for `ACCT` on the toy under `caps`, `INST_A` its one market, and `INST_B` beside
/// it when `two` holds, each under caps of 10 lots, its market and account lease names "toy".
fn registry(caps: &OrderCaps, two: bool) -> Registry {
    let toy = fbc_conformance::toy::caps();
    let symbol = |sym| dispatch_market_data(&toy, |s| s.venue_symbol(sym)).unwrap();
    let mut keys = LeaseKeys::new("toy", "test-acct", caps).with_market(INST_A, symbol(SYMBOL_A));
    let config = MarketCapsConfig {
        inventory: Lots::new(10),
        resting: Lots::new(10),
    };
    let mut market = PreTradeCaps::new().with_market(INST_A, config).unwrap();
    if two {
        keys = keys.with_market(INST_B, symbol(SYMBOL_B));
        market = market.with_market(INST_B, config).unwrap();
    }
    Registry::with_caps(market).with_lease_keys(keys)
}

/// A sweep and the notes of the link it has been handed so far.
struct Swept {
    sweep: Sweep,
    seen: usize,
}

/// What a scenario drives: the link, the session's orders and clock, the stub's venue and
/// pusher, the client-id mint, and the consumer's rate hold.
struct Run<'a> {
    link: Rc<RefCell<Link>>,
    orders: ExecOrders,
    clock: IngestClock,
    pusher: Pusher,
    venue: Shared,
    mint: CidMint,
    dir: &'a Dir,
    hold: RateHold,
    /// The retry time the test's venue states with a refusal for its rate (the toy's code 1006
    /// states none, so the consumer's wait holds unless a test sets one).
    retry_after: Option<Duration>,
}

impl Run<'_> {
    /// Lets the session and the stub run, the paused clock unmoved, until `done` holds; the test
    /// fails, naming `what`, once the watchdog's wall-clock time has passed.
    #[expect(
        clippy::disallowed_methods,
        reason = "the test's watchdog reads the wall clock; nothing the sweep times does"
    )]
    async fn until(&self, what: &str, done: impl Fn(&Run<'_>) -> bool) {
        let give_up = std::time::Instant::now() + WATCHDOG;
        while !done(self) {
            assert!(std::time::Instant::now() < give_up, "never {what}");
            tokio::task::yield_now().await;
        }
    }

    /// Whether the session's epoch takes places and the link has applied its resync.
    fn ready(&self) -> bool {
        let resynced = |n: &Note| matches!(n, Note::Resynced { .. });
        self.orders.may_place() && self.link.borrow().notes().iter().any(resynced)
    }

    /// Waits until the session is ready, then gives the owner's Start on `INST_A`.
    async fn started(&mut self) {
        self.until("ready", |r| r.ready()).await;
        let toy = fbc_conformance::toy::caps();
        let symbol = dispatch_market_data(&toy, |s| s.venue_symbol(SYMBOL_A)).unwrap();
        let market = MarketLease::acquire(&self.dir.0, "toy", "test-acct", &symbol).unwrap();
        let account = AccountLease::acquire(&self.dir.0, "toy", "test-acct").unwrap();
        let leases = Leases::market(market).with_account(account);
        self.link.borrow_mut().reg().start(INST_A, leases).unwrap();
    }

    /// The owner's Start on `INST_B` too, once `INST_A` started: its market lease, the account
    /// lease the registry already holds.
    fn started_b(&mut self) {
        let toy = fbc_conformance::toy::caps();
        let symbol = dispatch_market_data(&toy, |s| s.venue_symbol(SYMBOL_B)).unwrap();
        let market = MarketLease::acquire(&self.dir.0, "toy", "test-acct", &symbol).unwrap();
        let leases = Leases::market(market);
        self.link.borrow_mut().reg().start(INST_B, leases).unwrap();
    }

    /// A post-only order of a lot on `side` at `px` ticks on `INST_A`, authorized and submitted
    /// through the link, once the venue read it and, unless it holds acknowledgements, the
    /// registry holds it open: its client id.
    async fn place(&mut self, side: Side, px: i64) -> ClientOrderId {
        self.place_on(INST_A, side, px).await
    }

    /// [`Run::place`] on `inst`.
    async fn place_on(&mut self, inst: InstrumentId, side: Side, px: i64) -> ClientOrderId {
        let cid = self.mint.mint().unwrap();
        self.place_as_on(inst, cid, side, px).await;
        cid
    }

    /// The place of [`Run::place`] under client id `cid`, minted before: its request id.
    async fn place_as(&mut self, cid: ClientOrderId, side: Side, px: i64) -> RpcId {
        self.place_as_on(INST_A, cid, side, px).await
    }

    /// [`Run::place_as`] on `inst`.
    async fn place_as_on(
        &mut self,
        inst: InstrumentId,
        cid: ClientOrderId,
        side: Side,
        px: i64,
    ) -> RpcId {
        let placed = lock(&self.venue).orders.len() + 1;
        let rpc = self.submit(|reg| reg.place(new_order_on(inst, cid, side, px)).unwrap());
        self.until("the place read", |r| lock(&r.venue).orders.len() == placed)
            .await;
        if !lock(&self.venue).hold_acks {
            self.until("the place acknowledged", |r| r.state(cid) == OrdState::Open)
                .await;
        }
        rpc
    }

    /// The command `build` builds through the link's registry, authorized for `ACCT` and
    /// submitted through the link: its request id.
    fn submit(&self, build: impl FnOnce(&mut Registry) -> PermittedCommand) -> RpcId {
        let mut link = self.link.borrow_mut();
        let cmd = build(link.reg());
        let auth = link.reg().authorize(ACCT, cmd).unwrap();
        link.submit(&self.orders, auth).unwrap()
    }

    /// A single cancel of our order `cid`, the consumer's own, submitted through the link: its
    /// request id.
    fn cancel(&self, cid: ClientOrderId) -> RpcId {
        self.submit(|reg| reg.cancel_many(&[cid], &single_caps()).commands.remove(0))
    }

    /// The registry's state of our order `cid`.
    fn state(&self, cid: ClientOrderId) -> OrdState {
        self.link.borrow().registry().get(cid).unwrap().state()
    }

    /// The orders of ours the link holds live on `INST_A`.
    fn live(&self) -> Vec<ClientOrderId> {
        self.link.borrow().live_on(INST_A)
    }

    /// Whether the link noted an answer to request `rpc` of ours, or that it was not sent.
    fn answered(&self, rpc: RpcId) -> bool {
        let link = self.link.borrow();
        link.notes().iter().any(|n| match n {
            Note::Outcome { rpc: r, ours, .. } => *r == rpc && *ours,
            Note::Submitted { rpc: r, sent } => *r == rpc && sent.is_err(),
            _ => false,
        })
    }

    /// Sends `text` on the session's connection, as the venue would.
    async fn push(&self, text: String) {
        self.pusher.push(0, Frame::Text(text)).await.unwrap();
    }

    /// The venue's order `idx` ends `state`, unsolicited.
    async fn end_order(&self, idx: usize, state: &str) {
        let frame = lock(&self.venue).end(idx, state);
        self.push(frame).await;
    }

    /// The venue acknowledges the place of its order `idx`, held until now.
    async fn ack(&self, idx: usize) {
        let frame = {
            let mut v = lock(&self.venue);
            let o = &mut v.orders[idx];
            o.resting = true;
            format!("item|rpc={}|i=0|res=ok|vid={}", o.rpc, o.vid)
        };
        self.push(frame).await;
    }

    /// The venue answers the resync it held, showing the orders it holds resting.
    async fn resync(&self) {
        let records = {
            let v = lock(&self.venue);
            resync(v.resync_ts.as_deref().unwrap(), &v.orders, v.two_markets)
        };
        for record in records {
            self.push(record).await;
        }
    }

    /// The kinds of the requests the venue read after the opening (authentication, arm and
    /// resync), in order.
    fn kinds(&self) -> Vec<String> {
        let read = lock(&self.venue).read.clone();
        let opening = ["auth", "cod", "resync", "ping"];
        read.into_iter()
            .filter(|k| !opening.contains(&k.as_str()))
            .collect()
    }

    /// The session's shard clock's now, as the link reads it.
    fn now(&self) -> MonoNs {
        self.link.borrow().now().unwrap().0
    }

    /// Starts a sweep of `INST_A` for `ACCT` ending as `end` says, through the session's orders.
    fn start(&self, end: SweepEnd) -> (Swept, Round) {
        self.start_for(ACCT, &self.orders, end)
    }

    /// Starts a sweep of `INST_A` for `acct`, through `orders`.
    fn start_for(&self, acct: AccountKey, orders: &ExecOrders, end: SweepEnd) -> (Swept, Round) {
        let mut link = self.link.borrow_mut();
        let seen = link.notes().len();
        let (sweep, round) = Sweep::start(&mut link, orders, acct, INST_A, end);
        (Swept { sweep, seen }, round)
    }

    /// Hands `swept` the next note the link appended that it has not been handed, with the link
    /// and before any request is released, as a consumer reads them, recording in the consumer's
    /// hold each refusal for its rate at the venue's retry time: nothing else tells the sweep of
    /// a refusal. False when there is none.
    fn hand_one(&mut self, swept: &mut Swept) -> bool {
        let now = self.now();
        let link = self.link.borrow();
        let Some(note) = link.notes().get(swept.seen) else {
            return false;
        };
        swept.seen += 1;
        swept.sweep.observe(&link, note);
        if let Note::Outcome {
            outcome: SubmitOutcome::Rejected(r),
            ours: true,
            ..
        } = note
            && let RejectKind::RateLimited { retry_after } = r.kind
        {
            self.hold.refused(retry_after.or(self.retry_after), now);
        }
        true
    }

    /// Hands `swept` the notes the link appended since the last ([`Run::hand_one`]), then polls
    /// it at the session's clock's now.
    fn poll(&mut self, swept: &mut Swept) -> Polled {
        while self.hand_one(swept) {}
        let now = self.now();
        let mut link = self.link.borrow_mut();
        swept.sweep.poll(&mut link, &self.orders, &self.hold, now)
    }
}

/// A post-only order of a lot on `side` at `px` ticks on `INST_A`, as our client id `cid`.
fn new_order(cid: ClientOrderId, side: Side, px: i64) -> NewOrder {
    new_order_on(INST_A, cid, side, px)
}

/// [`new_order`] on `inst`.
fn new_order_on(inst: InstrumentId, cid: ClientOrderId, side: Side, px: i64) -> NewOrder {
    NewOrder {
        cid,
        inst,
        side,
        kind: OrderKind::Limit { px: Ticks(px) },
        qty: Lots::new(1).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// Runs `scenario` against the toy's order entry and the stub serving `venue`, the link and the
/// registry under `caps`, on a paused runtime nothing moves but the scenario.
fn play(caps: OrderCaps, venue: Venue, scenario: impl AsyncFnOnce(&mut Run<'_>)) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let dir = Dir::new();
    rt.block_on(async {
        // A blocked thread keeps the paused clock from moving on its own.
        let (thaw, frozen) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || frozen.recv());
        let clock = IngestClock::new();
        let outbound = venue.outbound.unwrap_or((64, 64));
        let two = venue.two_markets;
        let venue: Shared = Arc::new(Mutex::new(venue));
        let served = Arc::clone(&venue);
        let (script, pusher) = WsScript::serving(move |_conn| {
            let venue = Arc::clone(&served);
            Responder::new(move |frame| respond(&venue, frame))
        });
        let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
        let link = Rc::new(RefCell::new(link_over(registry(&caps, two), caps)));
        let handler = LinkHandler {
            link: Rc::clone(&link),
            wake: Box::new(|| {}),
        };
        let url = server.ws_url("/exec");
        let (mut session, control) = session_bounded(&url, &clock, handler, outbound);
        link.borrow_mut().attach(session.orders(), ACCT);
        let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
        let mut run = Run {
            link,
            orders: session.orders(),
            clock,
            pusher,
            venue,
            mint: CidMint::new(lease, 0, 0, WallNs(1_000_000_000)),
            dir: &dir,
            hold: RateHold::new(WAIT).unwrap(),
            retry_after: None,
        };
        let driven = async {
            scenario(&mut run).await;
            drop(control);
        };
        let (ran, ()) = tokio::join!(session.run(), driven);
        drop(thaw);
        ran.unwrap();
        assert!(server.failures().is_empty(), "{:?}", server.failures());
    });
}

/// fbc-oms's instrument cancel-all of `inst` under the toy's caps, the consumer's own.
fn cancel_all_of(reg: &mut Registry, inst: InstrumentId) -> PermittedCommand {
    let built = reg.cancel_everything(inst, &order_caps());
    let CancelEverything::CancelAll { command, .. } = built else {
        panic!("no cancel-all of {inst:?}: {built:?}");
    };
    command
}

/// `d` in nanoseconds.
fn ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap()
}

#[test]
fn a_start_sweep_sends_fbc_omss_instrument_cancel_all_and_ends_once_every_order_ended() {
    play(order_caps(), Venue::default(), async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let ask = run.place(Side::Sell, 110).await;
        // The link holds both live on their market, in the order it learnt them, and none on
        // another.
        assert_eq!(run.live(), [bid, ask]);
        assert!(run.link.borrow().live_on(INST_B).is_empty());

        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.how, How::CancelAll);
        assert!(round.failed.is_none());
        assert_eq!(round.sent.len(), 1);
        let rpc = round.sent[0].rpc;
        assert!(round.sent[0].names.is_empty());
        assert_eq!(swept.sweep.rpcs().to_vec(), [rpc]);
        // Before the venue answers, it waits, nothing due again, the request unanswered.
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        assert_eq!(swept.sweep.outcome(rpc), None);

        run.until("both orders ended", |r| {
            r.live().is_empty() && r.answered(rpc)
        })
        .await;
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, SweepState::Ended);
        assert!(polled.due.is_empty() && polled.due_failed.is_empty());
        assert!(polled.resent.is_none());
        assert!(matches!(
            swept.sweep.outcome(rpc),
            Some(SubmitOutcome::Accepted { .. })
        ));
        assert_eq!(swept.sweep.outcome(RpcId(999)), None);
        // One instrument cancel-all of the market, never an account cancel-all.
        assert_eq!(run.kinds(), ["place", "place", "cancelall"]);
        assert_eq!(lock(&run.venue).cancel_all_syms, [SYMBOL_A]);
    });
}

#[test]
fn once_a_cancel_not_ours_was_noted_the_sweep_cancels_ours_by_name_never_a_cancel_all() {
    play(order_caps(), Venue::default(), async |run| {
        run.started().await;
        let _bid = run.place(Side::Buy, 100).await;
        let ask = run.place(Side::Sell, 110).await;
        // Another program cancels the bid: no cancel of ours covers it.
        run.end_order(0, "canceled|why=requested").await;
        run.until("the cancel not ours noted", |r| {
            r.link.borrow().cancels_not_ours_seen()
        })
        .await;

        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.how, How::Explicit(Why::NotOursSeen));
        assert_eq!(round.sent.len(), 1);
        assert_eq!(round.sent[0].names, [ask]);
        let rpc = round.sent[0].rpc;
        run.until("the ask ended", |r| r.live().is_empty() && r.answered(rpc))
            .await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
        // A batch cancel naming the ask, and no instrument cancel-all at all.
        assert_eq!(run.kinds(), ["place", "place", "cancels"]);
    });
}

#[test]
fn with_no_cancel_all_from_fbc_oms_it_cancels_ours_by_name_and_again_after_a_rate_refusal() {
    // The bid's own cancel, sent before the sweep, is held, then refused for its rate; the
    // sweep's cancel of the ask is accepted; its cancel of the bid sent again is accepted.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent, Answer::Cancel, Answer::Cancel]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let ask = run.place(Side::Sell, 110).await;
        let cancel = {
            let mut link = run.link.borrow_mut();
            let choice = link.reg().cancellable(bid).unwrap().cancel(&order_caps());
            let CancelChoice::Send(cmd) = choice else {
                panic!("the toy cancels an acknowledged order by its venue id");
            };
            let auth = link.reg().authorize(ACCT, cmd).unwrap();
            link.submit(&run.orders, auth).unwrap()
        };
        run.until("the bid's cancel read", |r| r.kinds().len() == 3)
            .await;
        // The market disarmed: fbc-oms holds no exclusive lease, so it builds no cancel-all.
        run.link.borrow_mut().reg().disarm(INST_A);

        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        let refused = Why::Refused(CancelAllRefusal::NoExclusiveLease);
        assert_eq!(round.how, How::Explicit(refused));
        // The bid's cancel is in flight: only the ask is cancelled.
        assert_eq!(round.sent.len(), 1);
        assert_eq!(round.sent[0].names, [ask]);
        let rpc = round.sent[0].rpc;
        run.until("the ask ended", |r| r.live() == [bid] && r.answered(rpc))
            .await;
        assert_eq!(
            run.poll(&mut swept).state,
            SweepState::Waiting { resend_at: None }
        );

        // The bid's own cancel, not the sweep's, refused for its rate: the cancel of everything
        // is due again once the consumer's wait passed, and not before.
        run.push(format!("item|rpc={}|i=0|res=rej|code=1006", cancel.0))
            .await;
        run.until("the refusal noted", |r| r.answered(cancel)).await;
        let at = run.now();
        let polled = run.poll(&mut swept);
        let resend_at = Some(MonoNs(at.0 + ns(WAIT)));
        assert_eq!(polled.state, SweepState::Waiting { resend_at });
        assert!(polled.resent.is_none());
        tokio::time::advance(WAIT).await;
        let polled = run.poll(&mut swept);
        let resent = polled.resent.expect("the cancel of everything sent again");
        assert_eq!(resent.how, How::Explicit(refused));
        assert_eq!(resent.sent.len(), 1);
        assert_eq!(resent.sent[0].names, [bid]);
        let again = resent.sent[0].rpc;
        run.until("the bid ended", |r| {
            r.live().is_empty() && r.answered(again)
        })
        .await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
        assert_eq!(swept.sweep.rpcs().to_vec(), [rpc, again]);
        assert_eq!(
            run.kinds(),
            ["place", "place", "cancel", "cancels", "cancels"]
        );
    });
}

#[test]
fn a_cancel_all_refused_for_its_rate_is_sent_again_at_the_consumers_wait_and_not_before() {
    let venue = Venue {
        cancel_alls: VecDeque::from([Answer::Refuse("1006"), Answer::Cancel]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        run.place(Side::Buy, 100).await;
        run.place(Side::Sell, 110).await;
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        let first = round.sent[0].rpc;
        run.until("the refusal noted", |r| r.answered(first)).await;
        let at = run.now();
        let polled = run.poll(&mut swept);
        let until = MonoNs(at.0 + ns(WAIT));
        assert_eq!(run.hold.until(), Some(until));
        let waiting = SweepState::Waiting {
            resend_at: Some(until),
        };
        assert_eq!(polled.state, waiting);
        assert!(polled.resent.is_none());
        // A nanosecond before the wait ends, still held; at its end, sent again.
        tokio::time::advance(WAIT - Duration::from_nanos(1)).await;
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, waiting);
        assert!(polled.resent.is_none());
        tokio::time::advance(Duration::from_nanos(1)).await;
        let polled = run.poll(&mut swept);
        let resent = polled.resent.expect("sent again at the wait's end");
        assert_eq!(resent.how, How::CancelAll);
        let second = resent.sent[0].rpc;
        run.until("both orders ended", |r| {
            r.live().is_empty() && r.answered(second)
        })
        .await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
        assert_eq!(swept.sweep.rpcs().to_vec(), [first, second]);
        let refused = swept.sweep.outcome(first);
        assert!(
            matches!(&refused, Some(SubmitOutcome::Rejected(r))
                if matches!(r.kind, RejectKind::RateLimited { .. })),
            "{refused:?}"
        );
        assert_eq!(run.kinds(), ["place", "place", "cancelall", "cancelall"]);
        assert_eq!(lock(&run.venue).cancel_all_syms, [SYMBOL_A, SYMBOL_A]);
    });
}

#[test]
fn a_venues_retry_time_holds_the_resend_until_it_ends_not_until_the_consumers_wait() {
    let venue = Venue {
        cancel_alls: VecDeque::from([Answer::Refuse("1006"), Answer::Cancel]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        // As a venue that states its retry time with the refusal would: half a second.
        let retry = Duration::from_millis(500);
        run.retry_after = Some(retry);
        run.started().await;
        run.place(Side::Buy, 100).await;
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        let first = round.sent[0].rpc;
        run.until("the refusal noted", |r| r.answered(first)).await;
        let at = run.now();
        let until = MonoNs(at.0 + ns(retry));
        let polled = run.poll(&mut swept);
        let resend_at = Some(until);
        assert_eq!(polled.state, SweepState::Waiting { resend_at });
        tokio::time::advance(retry - Duration::from_nanos(1)).await;
        assert!(run.poll(&mut swept).resent.is_none());
        tokio::time::advance(Duration::from_nanos(1)).await;
        let resent = run
            .poll(&mut swept)
            .resent
            .expect("sent again at the retry time");
        let second = resent.sent[0].rpc;
        assert!(run.now() < MonoNs(at.0 + ns(WAIT)));
        run.until("the order ended", |r| {
            r.live().is_empty() && r.answered(second)
        })
        .await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
    });
}

#[test]
fn before_places_a_cancel_of_an_order_already_ended_waits_for_that_end() {
    // A batch of two: the bid's item refused AlreadyTerminal, the ask's accepted.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Refuse("2002"), Answer::Cancel]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let ask = run.place(Side::Sell, 110).await;
        run.link.borrow_mut().reg().disarm(INST_A);
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.sent.len(), 1);
        assert_eq!(round.sent[0].names, [bid, ask]);
        let rpc = round.sent[0].rpc;
        run.until("the ask ended", |r| r.live() == [bid] && r.answered(rpc))
            .await;
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        // The batch's outcome is its first item not accepted.
        let outcome = swept.sweep.outcome(rpc);
        assert!(
            matches!(&outcome, Some(SubmitOutcome::Rejected(r))
                if matches!(r.kind, RejectKind::AlreadyTerminal(_))),
            "{outcome:?}"
        );
        // Its end comes: the sweep ends.
        run.end_order(0, "expired").await;
        run.until("the bid ended", |r| r.live().is_empty()).await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
    });
}

#[test]
fn before_places_any_other_refusal_fails_the_sweep_and_at_stop_none_ends_it_early() {
    let venue = Venue {
        cancel_alls: VecDeque::from([Answer::Refuse("9999"), Answer::Refuse("9999")]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        run.place(Side::Buy, 100).await;
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        let rpc = round.sent[0].rpc;
        run.until("the refusal noted", |r| r.answered(rpc)).await;
        for _ in 0..2 {
            let polled = run.poll(&mut swept);
            let SweepState::Failed(reject) = &polled.state else {
                panic!("not failed: {:?}", polled.state);
            };
            assert_eq!(reject.kind, RejectKind::Other);
            assert!(polled.due.is_empty() && polled.resent.is_none());
        }

        // At Stop the same refusal ends nothing: the sweep waits for the order's end.
        run.link.borrow_mut().reg().kill(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.how, How::CancelAll);
        let rpc = round.sent[0].rpc;
        run.until("the refusal noted", |r| r.answered(rpc)).await;
        let polled = run.poll(&mut stop);
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        run.end_order(0, "expired").await;
        run.until("the order ended", |r| r.live().is_empty()).await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        let outcome = stop.sweep.outcome(rpc);
        assert!(
            matches!(&outcome, Some(SubmitOutcome::Rejected(r)) if r.kind == RejectKind::Other),
            "{outcome:?}"
        );
    });
}

#[test]
fn an_order_a_not_found_puts_on_the_unknown_ladder_keeps_the_sweep_waiting() {
    let venue = Venue {
        cancels: VecDeque::from([Answer::Refuse("2001")]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        run.link.borrow_mut().reg().disarm(INST_A);
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.sent[0].names, [bid]);
        let rpc = round.sent[0].rpc;
        run.until("the refusal noted", |r| r.answered(rpc)).await;
        for _ in 0..2 {
            let polled = run.poll(&mut swept);
            assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
            assert!(polled.due.is_empty() && polled.resent.is_none());
        }
        // fbc-oms holds it on the Unknown ladder, not ended: the sweep queries no order.
        let link = run.link.borrow();
        let record = link.registry().get(bid).unwrap();
        assert!(record.unknown_since().is_some());
        assert!(!record.state().is_terminal());
        assert_eq!(link.live_on(INST_A), [bid]);
    });
}

#[test]
fn before_places_the_sweep_waits_for_the_market_to_take_places_and_at_stop_it_does_not() {
    let venue = Venue {
        hold_resync: true,
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.until("the resync asked for", |r| {
            lock(&r.venue).resync_ts.is_some()
        })
        .await;
        assert!(!run.orders.may_place_on(INST_A));
        // No order of ours, and a market not armed: explicit cancels of nothing.
        let (mut before, round) = run.start(SweepEnd::BeforePlaces);
        let refused = Why::Refused(CancelAllRefusal::NoExclusiveLease);
        assert_eq!(round.how, How::Explicit(refused));
        assert!(round.sent.is_empty() && round.failed.is_none());
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert!(round.sent.is_empty());
        assert_eq!(
            run.poll(&mut before).state,
            SweepState::Waiting { resend_at: None }
        );
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert!(!run.orders.may_place_on(INST_A));

        run.resync().await;
        run.until("ready", |r| r.ready()).await;
        assert_eq!(run.poll(&mut before).state, SweepState::Ended);
    });
}

#[test]
fn at_stop_the_sweep_ends_only_once_every_request_it_sent_is_answered() {
    // The cancel-all's order events come before its answer.
    let venue = Venue {
        cancel_alls: VecDeque::from([Answer::EndOnly]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        run.place(Side::Buy, 100).await;
        run.link.borrow_mut().reg().kill(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        let rpc = round.sent[0].rpc;
        run.until("the order ended", |r| r.live().is_empty()).await;
        assert_eq!(
            run.poll(&mut stop).state,
            SweepState::Waiting { resend_at: None }
        );
        run.push(format!("item|rpc={}|i=0|res=ok", rpc.0)).await;
        run.until("the answer noted", |r| r.answered(rpc)).await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
    });
}

#[test]
fn a_cancel_that_waited_for_its_orders_acknowledgement_is_sent_once_it_landed() {
    let venue = Venue {
        hold_acks: true,
        ..Venue::default()
    };
    play(awaiting_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        assert_eq!(run.state(bid), OrdState::PendingNew);
        // The instrument cancel-all; the bid's own cancel waits for its acknowledgement.
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.how, How::CancelAll);
        assert_eq!(round.sent.len(), 1);
        let all = round.sent[0].rpc;
        run.until("the cancel-all answered", |r| r.answered(all))
            .await;
        let polled = run.poll(&mut swept);
        assert!(polled.due.is_empty());
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });

        run.ack(0).await;
        run.until("the bid acknowledged", |r| r.state(bid) == OrdState::Open)
            .await;
        let polled = run.poll(&mut swept);
        assert!(polled.due_failed.is_empty());
        assert_eq!(polled.due.len(), 1);
        assert_eq!(polled.due[0].names, [bid]);
        let due = polled.due[0].rpc;
        run.until("the bid ended", |r| r.live().is_empty() && r.answered(due))
            .await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
        assert_eq!(swept.sweep.rpcs().to_vec(), [all, due]);
        assert_eq!(run.kinds(), ["place", "cancelall", "cancel"]);
    });
}

#[test]
fn a_sweep_names_an_order_the_venue_has_not_acknowledged_by_its_placement_nonce_at_once() {
    // The toy's batch cancel names an order by its placement nonce, which fbc-oms takes before
    // the acknowledgement (design §4.9) once the link has recorded it from the place's receipt
    // (FBC-o0f0): the first round names the bid by it in a batch cancel beside the instrument
    // cancel-all. With no nonce recorded the batch cannot name it, and the round names it in a
    // single cancel by our client id instead. The test's venue knows its orders by venue id and
    // client id, so in front of it the test keeps each place's nonce and client id, and reads a
    // batch cancel item naming a nonce as naming that place's order. This is `play` with that
    // responder; no nonce's value is printed.
    fn held<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
        shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let dir = Dir::new();
    // Each place's nonce and client id, and each batch cancel item as the session wrote it.
    let placed: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let items: Arc<Mutex<Vec<String>>> = Arc::default();
    rt.block_on(async {
        let (thaw, frozen) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || frozen.recv());
        let clock = IngestClock::new();
        let venue = Venue {
            hold_acks: true,
            ..Venue::default()
        };
        let venue: Shared = Arc::new(Mutex::new(venue));
        let served = (Arc::clone(&venue), Arc::clone(&placed), Arc::clone(&items));
        let (script, pusher) = WsScript::serving(move |_conn| {
            let (venue, placed, items) = (
                Arc::clone(&served.0),
                Arc::clone(&served.1),
                Arc::clone(&served.2),
            );
            Responder::new(move |frame| {
                let Frame::Text(text) = frame else {
                    return respond(&venue, frame);
                };
                let mut lines = text.lines();
                let head = lines.next().unwrap_or_default();
                match head.split('|').next().unwrap_or_default() {
                    "place" => {
                        let place = (field(head, "nonce"), field(head, "cid"));
                        held(&placed).push(place);
                        respond(&venue, frame)
                    }
                    "cancels" => {
                        let mut read = vec![head.to_owned()];
                        for line in lines {
                            held(&items).push(line.to_owned());
                            let mut line = line.to_owned();
                            for (nonce, cid) in held(&placed).iter() {
                                line = line
                                    .replace(&format!("|pnonce={nonce}|"), &format!("|cid={cid}|"));
                            }
                            read.push(line);
                        }
                        respond(&venue, &Frame::Text(read.join("\n")))
                    }
                    _ => respond(&venue, frame),
                }
            })
        });
        let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
        let link = Rc::new(RefCell::new(link_over(
            registry(&order_caps(), false),
            order_caps(),
        )));
        let handler = LinkHandler {
            link: Rc::clone(&link),
            wake: Box::new(|| {}),
        };
        let url = server.ws_url("/exec");
        let (mut session, control) = session(&url, &clock, handler);
        link.borrow_mut().attach(session.orders(), ACCT);
        let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
        let mut run = Run {
            link,
            orders: session.orders(),
            clock,
            pusher,
            venue,
            mint: CidMint::new(lease, 0, 0, WallNs(1_000_000_000)),
            dir: &dir,
            hold: RateHold::new(WAIT).unwrap(),
            retry_after: None,
        };
        let driven = async {
            let run = &mut run;
            run.started().await;
            let bid = run.place(Side::Buy, 100).await;
            assert_eq!(run.state(bid), OrdState::PendingNew);
            let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
            assert_eq!(round.how, How::CancelAll);
            assert!(round.failed.is_none());
            // The cancel-all, then the batch cancel naming the bid, both in the first round.
            assert_eq!(round.sent.len(), 2);
            assert!(round.sent[0].names.is_empty());
            assert_eq!(round.sent[1].names, [bid]);
            let (all, by_nonce) = (round.sent[0].rpc, round.sent[1].rpc);
            run.until("the bid ended", |r| {
                r.live().is_empty() && r.answered(all) && r.answered(by_nonce)
            })
            .await;
            let polled = run.poll(&mut swept);
            assert!(polled.due.is_empty() && polled.due_failed.is_empty());
            assert_eq!(polled.state, SweepState::Ended);
            assert_eq!(swept.sweep.rpcs().to_vec(), [all, by_nonce]);
            assert_eq!(run.kinds(), ["place", "cancelall", "cancels"]);
            // Its one item named the bid by the nonce its place was sent with, and by nothing
            // else.
            let items = held(&items);
            assert_eq!(items.len(), 1);
            let (nonce, _) = &held(&placed)[0];
            assert!(
                items[0].contains(&format!("|pnonce={nonce}|")),
                "the item names the bid's placement nonce"
            );
            assert!(!items[0].contains("|vid=") && !items[0].contains("|cid="));
            drop(control);
        };
        let (ran, ()) = tokio::join!(session.run(), driven);
        drop(thaw);
        ran.unwrap();
        assert!(server.failures().is_empty(), "{:?}", server.failures());
    });
}

#[test]
fn a_cancel_fbc_oms_does_not_authorize_for_the_sweeps_account_is_said_never_sent() {
    let venue = Venue {
        hold_acks: true,
        ..Venue::default()
    };
    play(awaiting_caps(), venue, async |run| {
        run.started().await;
        run.place(Side::Buy, 100).await;
        // A sweep for an account the registry is not for: its cancel-all is not authorized, and
        // nothing after it is sent.
        let orders = run.orders.clone();
        let (mut wrong, round) = run.start_for(AccountKey::new(2), &orders, SweepEnd::AtStop);
        assert_eq!(round.how, How::CancelAll);
        assert!(round.sent.is_empty());
        assert!(matches!(round.failed, Some(SendError::Unauthorized(_))));
        // Nor is the bid's cancel once its acknowledgement landed.
        run.ack(0).await;
        run.until("the bid acknowledged", |r| {
            r.link
                .borrow()
                .registry()
                .cancels_due(&awaiting_caps())
                .len()
                == 1
        })
        .await;
        let polled = run.poll(&mut wrong);
        assert!(polled.due.is_empty());
        assert_eq!(polled.due_failed.len(), 1);
        assert!(matches!(polled.due_failed[0], SendError::Unauthorized(_)));
        assert_eq!(
            polled.state,
            SweepState::Waiting { resend_at: None },
            "the bid still rests"
        );
        assert!(wrong.sweep.rpcs().is_empty());
        assert_eq!(run.kinds(), ["place"]);
    });
}

#[test]
fn a_cancel_the_session_does_not_take_or_does_not_send_is_said_so() {
    play(order_caps(), Venue::default(), async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        // A session that has ended takes nothing: the round stops at its cancel-all.
        let dead = {
            let handler = LinkHandler {
                link: Rc::new(RefCell::new(link_over(Registry::new(), order_caps()))),
                wake: Box::new(|| {}),
            };
            let (gone, _control) = session("ws://127.0.0.1:9/", &run.clock, handler);
            gone.orders()
        };
        let (_, round) = run.start_for(ACCT, &dead, SweepEnd::AtStop);
        assert!(round.sent.is_empty());
        let Some(SendError::Unsubmitted(why)) = &round.failed else {
            panic!("submitted: {:?}", round.failed);
        };
        assert_eq!(why, "Ended");

        // The venue closes the connection: fbc-oms has not viewed the account since, so the
        // cancel is explicit, and with no connection the session does not send it.
        run.pusher.close(0).await.unwrap();
        let ended = |n: &Note| matches!(n, Note::EpochEnd { .. });
        run.until("the connection's end", |r| {
            r.link.borrow().notes().iter().any(ended)
        })
        .await;
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        let refused = Why::Refused(CancelAllRefusal::NotViewed);
        assert_eq!(round.how, How::Explicit(refused));
        assert_eq!(round.sent[0].names, [bid]);
        let rpc = round.sent[0].rpc;
        run.until("reported not sent", |r| r.answered(rpc)).await;
        let polled = run.poll(&mut stop);
        assert!(matches!(
            stop.sweep.outcome(rpc),
            Some(SubmitOutcome::NotSent(NotSentReason::Disconnected))
        ));
        // Answered, but the bid still rests: Stop's sweep waits on, for its consumer to bound.
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
    });
}

#[test]
fn the_rate_hold_holds_for_the_venues_retry_time_or_the_consumers_wait_and_refuses_zero() {
    assert_eq!(RateHold::new(Duration::ZERO), Err(ZeroWait));
    assert_eq!(
        ZeroWait.to_string(),
        "the rate hold's wait when the venue states no retry time is zero"
    );
    let error: &dyn std::error::Error = &ZeroWait;
    assert!(error.source().is_none());

    let mut hold = RateHold::new(WAIT).unwrap();
    assert_eq!(hold.wait(), WAIT);
    assert_eq!(hold.until(), None);
    assert!(!hold.held(MonoNs(0)));
    // No retry time stated: the consumer's wait.
    assert_eq!(hold.refused(None, MonoNs(1_000)), WAIT);
    let ends = MonoNs(1_000 + ns(WAIT));
    assert_eq!(hold.until(), Some(ends));
    assert!(hold.held(MonoNs(ends.0 - 1)));
    assert!(!hold.held(ends));
    // A shorter retry time stated later never ends it sooner.
    let short = Duration::from_millis(1);
    assert_eq!(hold.refused(Some(short), MonoNs(2_000)), short);
    assert_eq!(hold.until(), Some(ends));
    // A longer one holds it longer.
    let long = Duration::from_secs(5);
    assert_eq!(hold.refused(Some(long), MonoNs(3_000)), long);
    assert_eq!(hold.until(), Some(MonoNs(3_000 + ns(long))));
    // One past the clock's nanoseconds holds to its end.
    hold.refused(Some(Duration::MAX), MonoNs(1));
    assert_eq!(hold.until(), Some(MonoNs(u64::MAX)));
    assert!(hold.held(MonoNs(u64::MAX - 1)));
}

#[test]
fn the_explicit_cancels_name_our_orders_in_the_order_the_link_learnt_them() {
    play(order_caps(), Venue::default(), async |run| {
        run.started().await;
        let first = run.mint.mint().unwrap();
        let second = run.mint.mint().unwrap();
        assert!(first < second);
        // The later client id placed first: the link learns it first.
        run.place_as(second, Side::Sell, 110).await;
        run.place_as(first, Side::Buy, 100).await;
        assert_eq!(run.live(), [second, first]);
        run.link.borrow_mut().reg().disarm(INST_A);

        let (mut stop, round) = run.start(SweepEnd::AtStop);
        let refused = Why::Refused(CancelAllRefusal::NoExclusiveLease);
        assert_eq!(round.how, How::Explicit(refused));
        assert_eq!(round.sent.len(), 1);
        // One batch, its items in the order the link learnt the orders, not client id order.
        assert_eq!(round.sent[0].names, [second, first]);
        let rpc = round.sent[0].rpc;
        run.until("both ended", |r| r.live().is_empty() && r.answered(rpc))
            .await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert_eq!(run.kinds(), ["place", "place", "cancels"]);
    });
}

#[test]
fn a_cancel_of_ours_refused_for_its_rate_after_its_order_ended_makes_the_resend_due() {
    // The bid's own cancel, the consumer's, sent before Stop, is held, and so is Stop's
    // cancel-all; the venue ends the bid itself, then refuses the bid's cancel for its rate while
    // the ask rests.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent]),
        cancel_alls: VecDeque::from([Answer::Silent]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let ask = run.place(Side::Sell, 110).await;
        let cancel = run.cancel(bid);
        run.until("the bid's cancel read", |r| r.kinds().len() == 3)
            .await;
        run.link.borrow_mut().reg().kill(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.how, How::CancelAll);
        let all = round.sent[0].rpc;
        run.until("the cancel-all read", |r| r.kinds().len() == 4)
            .await;
        run.end_order(0, "expired").await;
        run.until("the bid ended", |r| r.live() == [ask]).await;

        // Refused for its rate once its order ended: only the note, handed over with the link,
        // which says the request is a cancel, tells the sweep. The cancel of everything is due
        // again at the consumer's wait, and sent then.
        run.push(format!("item|rpc={}|i=0|res=rej|code=1006", cancel.0))
            .await;
        run.until("the refusal noted", |r| r.answered(cancel)).await;
        assert!(run.link.borrow().is_cancel_on(cancel, INST_A));
        let at = run.now();
        let polled = run.poll(&mut stop);
        let resend_at = Some(MonoNs(at.0 + ns(WAIT)));
        assert_eq!(polled.state, SweepState::Waiting { resend_at });
        assert!(polled.resent.is_none());
        tokio::time::advance(WAIT).await;
        let polled = run.poll(&mut stop);
        let resent = polled.resent.expect("the cancel of everything sent again");
        assert_eq!(resent.how, How::CancelAll);
        let again = resent.sent[0].rpc;
        run.until("the ask ended", |r| {
            r.live().is_empty() && r.answered(again)
        })
        .await;
        // Stop's first cancel-all is still unanswered: Stop waits for it.
        assert_eq!(
            run.poll(&mut stop).state,
            SweepState::Waiting { resend_at: None }
        );
        run.push(format!("item|rpc={}|i=0|res=ok", all.0)).await;
        run.until("the first cancel-all answered", |r| r.answered(all))
            .await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert_eq!(stop.sweep.rpcs().to_vec(), [all, again]);
        assert_eq!(
            run.kinds(),
            ["place", "place", "cancel", "cancelall", "cancelall"]
        );
    });
}

#[test]
fn a_place_refused_for_its_rate_during_a_sweep_makes_nothing_due() {
    // The sweep's cancel-all is held; a place sent beside it is refused for its rate.
    let venue = Venue {
        cancel_alls: VecDeque::from([Answer::Silent]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        run.place(Side::Buy, 100).await;
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.how, How::CancelAll);
        let all = round.sent[0].rpc;
        run.until("the cancel-all read", |r| r.kinds().len() == 2)
            .await;
        lock(&run.venue).hold_acks = true;
        let ask = run.mint.mint().unwrap();
        let place = run.place_as(ask, Side::Sell, 110).await;
        assert!(!run.link.borrow().is_cancel_on(place, INST_A));
        run.push(format!("item|rpc={}|i=0|res=rej|code=1006", place.0))
            .await;
        run.until("the refusal noted", |r| r.answered(place)).await;
        // The consumer's hold records it, and nothing is due, then or once the hold passed.
        let at = run.now();
        let polled = run.poll(&mut swept);
        assert_eq!(run.hold.until(), Some(MonoNs(at.0 + ns(WAIT))));
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        assert!(polled.resent.is_none());
        tokio::time::advance(WAIT).await;
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        assert!(polled.resent.is_none());

        // The cancel-all answered and the bid ended: the sweep ends, nothing sent again.
        run.push(format!("item|rpc={}|i=0|res=ok", all.0)).await;
        run.end_order(0, "canceled|why=requested").await;
        run.until("the bid ended", |r| r.live().is_empty() && r.answered(all))
            .await;
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, SweepState::Ended);
        assert!(polled.resent.is_none());
        assert_eq!(swept.sweep.rpcs().to_vec(), [all]);
        assert_eq!(run.kinds(), ["place", "cancelall", "place"]);
    });
}

#[test]
fn a_safety_cancel_the_link_sent_refused_for_its_rate_makes_the_resend_due() {
    // The venue's first cancel, the link's Safety cancel, is held.
    let venue = Venue {
        hold_resync: true,
        cancels: VecDeque::from([Answer::Silent]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.until("the resync asked for", |r| {
            lock(&r.venue).resync_ts.is_some()
        })
        .await;
        // An order of ours from an earlier run rests. The resync shows it, and since the toy's
        // arm covers only the orders placed after it, the session tells it unprotected: the link
        // sends its Safety cancel at once.
        let earlier = run.mint.mint().unwrap();
        let wire = encode_cid(&order_caps().client_id, earlier).unwrap();
        let line = format!("place|cid={wire}|side=B|px=100|qty=1");
        lock(&run.venue)
            .orders
            .push(Order::placed(&line, "V-1".to_owned(), String::new(), true));
        run.resync().await;
        run.until("its Safety cancel read", |r| r.kinds() == ["cancels"])
            .await;
        let safety = run.link.borrow().safety_cancel_of(earlier).unwrap();
        assert!(run.link.borrow().is_cancel_on(safety, INST_A));
        assert!(!run.orders.may_place_on(INST_A));

        // The sweep leaves the order out of its explicit cancels: its Safety cancel is in flight.
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        let refused = Why::Refused(CancelAllRefusal::NoExclusiveLease);
        assert_eq!(round.how, How::Explicit(refused));
        assert!(round.sent.is_empty());
        assert_eq!(run.live(), [earlier]);
        assert_eq!(
            run.poll(&mut swept).state,
            SweepState::Waiting { resend_at: None }
        );

        // The Safety cancel refused for its rate: the cancel of everything is due again at the
        // consumer's wait, and sent then.
        run.push(format!("item|rpc={}|i=0|res=rej|code=1006", safety.0))
            .await;
        run.until("the refusal noted", |r| r.answered(safety)).await;
        let at = run.now();
        let polled = run.poll(&mut swept);
        let resend_at = Some(MonoNs(at.0 + ns(WAIT)));
        assert_eq!(polled.state, SweepState::Waiting { resend_at });
        assert!(polled.resent.is_none());
        tokio::time::advance(WAIT).await;
        let resent = run
            .poll(&mut swept)
            .resent
            .expect("the cancel of everything sent again");
        assert_eq!(resent.how, How::Explicit(refused));
        assert_eq!(resent.sent.len(), 1);
        assert_eq!(resent.sent[0].names, [earlier]);
        let again = resent.sent[0].rpc;
        run.until("the order ended", |r| {
            r.live().is_empty() && r.answered(again) && r.orders.may_place_on(INST_A)
        })
        .await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
        assert_eq!(swept.sweep.rpcs().to_vec(), [again]);
        assert_eq!(run.kinds(), ["cancels", "cancels"]);
    });
}

#[test]
fn the_link_says_which_requests_it_holds_are_cancels_until_each_is_released() {
    // Nothing a cancel names ends: each is left unanswered.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent, Answer::Silent]),
        cancel_alls: VecDeque::from([Answer::Silent]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.mint.mint().unwrap();
        let place = run.place_as(bid, Side::Buy, 100).await;
        // A batch of two places, acknowledged.
        let (one, two) = (run.mint.mint().unwrap(), run.mint.mint().unwrap());
        let orders = vec![
            new_order(one, Side::Sell, 110),
            new_order(two, Side::Sell, 111),
        ];
        let batch = run.submit(|reg| reg.place_batch(orders).unwrap().command.unwrap());
        run.until("the batch acknowledged", |r| {
            r.state(one) == OrdState::Open && r.state(two) == OrdState::Open
        })
        .await;
        // An amend of the bid, a cancel of the batch's first order, a cancel-many of its second,
        // and the market's instrument cancel-all, a sweep's.
        let amend = run.submit(|reg| {
            let live = reg.live(bid).unwrap();
            live.amend(&order_caps(), Ticks(99), Lots::new(1).unwrap(), false)
                .unwrap()
        });
        let cancel = run.cancel(one);
        let many = run.submit(|reg| reg.cancel_many(&[two], &order_caps()).commands.remove(0));
        let (_, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.how, How::CancelAll);
        let all = round.sent[0].rpc;
        run.until("every request read", |r| r.kinds().len() == 6)
            .await;
        assert_eq!(
            run.kinds(),
            ["place", "batch", "amend", "cancel", "cancels", "cancelall"]
        );

        let mut link = run.link.borrow_mut();
        for rpc in [place, batch, amend] {
            assert!(!link.is_cancel_on(rpc, INST_A), "request {}", rpc.0);
        }
        for rpc in [cancel, many, all] {
            assert!(link.is_cancel_on(rpc, INST_A), "request {}", rpc.0);
        }
        // A request the link never submitted: the session's own arm, and one never made.
        let arm = link.notes().iter().find_map(|n| match n {
            Note::Outcome {
                rpc, ours: false, ..
            } => Some(*rpc),
            _ => None,
        });
        let arm = arm.expect("the arm answered");
        assert!(!link.is_cancel_on(arm, INST_A));
        assert!(!link.is_cancel_on(RpcId(999), INST_A));
        // Released, no request is a cancel any more.
        for rpc in [place, batch, amend, cancel, many, all] {
            link.release(rpc);
            assert!(
                !link.is_cancel_on(rpc, INST_A),
                "request {} released",
                rpc.0
            );
        }
        assert_eq!(link.requests_held(), 0);
    });
}

#[test]
fn at_stop_the_sweep_waits_for_every_request_it_sent_not_the_first_alone() {
    // The cancel-all answered at once; the bid's cancel, due on its acknowledgement, ends it
    // with its answer held.
    let venue = Venue {
        hold_acks: true,
        cancels: VecDeque::from([Answer::EndOnly]),
        ..Venue::default()
    };
    play(awaiting_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        run.link.borrow_mut().reg().kill(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.sent.len(), 1);
        let all = round.sent[0].rpc;
        run.until("the cancel-all answered", |r| r.answered(all))
            .await;
        run.ack(0).await;
        run.until("the bid acknowledged", |r| r.state(bid) == OrdState::Open)
            .await;
        let polled = run.poll(&mut stop);
        assert_eq!(polled.due.len(), 1);
        let due = polled.due[0].rpc;
        run.until("the bid ended", |r| r.live().is_empty()).await;
        // Every order ended and the first request answered: the second still waits.
        assert_eq!(
            run.poll(&mut stop).state,
            SweepState::Waiting { resend_at: None }
        );
        run.push(format!("item|rpc={}|i=0|res=ok", due.0)).await;
        run.until("the due cancel answered", |r| r.answered(due))
            .await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert_eq!(stop.sweep.rpcs().to_vec(), [all, due]);
    });
}

#[test]
fn a_batch_is_judged_once_every_item_is_answered_by_its_first_item_not_accepted() {
    // The bid's item accepted, the ask's refused AlreadyTerminal.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Cancel, Answer::Refuse("2002")]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let ask = run.place(Side::Sell, 110).await;
        run.link.borrow_mut().reg().disarm(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.sent[0].names, [bid, ask]);
        let rpc = round.sent[0].rpc;
        let items = |r: &Run<'_>| {
            let link = r.link.borrow();
            let of = |n: &&Note| matches!(n, Note::Outcome { rpc: x, .. } if *x == rpc);
            link.notes().iter().filter(of).count()
        };
        run.until("both items answered", |r| items(r) == 2).await;
        // Handed as a consumer reading one note at a time: after the first item's answer, an
        // accepted one, the batch is not judged.
        loop {
            let first = {
                let link = run.link.borrow();
                let next = link.notes().get(stop.seen);
                matches!(next, Some(Note::Outcome { rpc: x, item: Some(0), .. }) if *x == rpc)
            };
            assert!(run.hand_one(&mut stop));
            if first {
                break;
            }
        }
        assert_eq!(stop.sweep.outcome(rpc), None);
        while run.hand_one(&mut stop) {}
        let outcome = stop.sweep.outcome(rpc);
        assert!(
            matches!(&outcome, Some(SubmitOutcome::Rejected(r))
                if matches!(r.kind, RejectKind::AlreadyTerminal(_))),
            "{outcome:?}"
        );
    });
}

#[test]
fn a_whole_request_answer_is_the_requests_outcome() {
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        run.link.borrow_mut().reg().disarm(INST_A);
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.sent[0].names, [bid]);
        let rpc = round.sent[0].rpc;
        run.until("the cancel read", |r| r.kinds().len() == 2).await;
        // The venue refuses the whole request: its order already ended. It waits for that end.
        run.push(format!("reject|rpc={}|code=2002", rpc.0)).await;
        run.until("the refusal noted", |r| r.answered(rpc)).await;
        assert_eq!(
            run.poll(&mut swept).state,
            SweepState::Waiting { resend_at: None }
        );
        let outcome = swept.sweep.outcome(rpc);
        assert!(
            matches!(&outcome, Some(SubmitOutcome::Rejected(r))
                if matches!(r.kind, RejectKind::AlreadyTerminal(_))),
            "{outcome:?}"
        );
        run.end_order(0, "expired").await;
        run.until("the bid ended", |r| r.live().is_empty()).await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
    });
}

#[test]
fn a_round_that_stops_short_leaves_what_it_sent_before_to_the_consumer() {
    // The session takes one command between its turns: the cancel-all waits there, and the
    // explicit cancel beside it of the bid, not yet acknowledged, is refused. The cancel-all is
    // never answered.
    let venue = Venue {
        hold_acks: true,
        cancel_alls: VecDeque::from([Answer::Silent]),
        outbound: Some((1, 0)),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        run.link.borrow_mut().reg().kill(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.how, How::CancelAll);
        assert_eq!(round.sent.len(), 1);
        assert!(round.sent[0].names.is_empty());
        let Some(SendError::Unsubmitted(why)) = &round.failed else {
            panic!("handed over: {:?}", round.failed);
        };
        assert!(why.contains("Backpressure"), "{why}");
        // Neither listed nor waited on: Stop's consumer handles the round's failure.
        assert!(stop.sweep.rpcs().is_empty());
        let all = round.sent[0].rpc;
        assert_eq!(stop.sweep.outcome(all), None);
        run.until("the cancel-all read", |r| r.kinds().len() == 2)
            .await;
        run.ack(0).await;
        run.until("the bid acknowledged", |r| r.state(bid) == OrdState::Open)
            .await;
        run.end_order(0, "expired").await;
        run.until("the bid ended", |r| r.live().is_empty()).await;
        assert!(!run.answered(all));
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        // Nothing after the refused cancel was sent.
        assert_eq!(run.kinds(), ["place", "cancelall"]);
    });
}

#[test]
fn a_later_resync_showing_our_orders_in_another_order_leaves_the_order_the_link_learnt_them() {
    // After the reconnect, the resync shows the ask before the bid, and the session tells both
    // unprotected: the link's Safety cancel of them is refused for its rate, so neither has a
    // cancel in flight when the sweep starts.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Refuse("1006"), Answer::Refuse("1006")]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let ask = run.place(Side::Sell, 110).await;
        assert_eq!(run.live(), [bid, ask]);
        lock(&run.venue).resync_reversed = true;
        run.pusher.close(0).await.unwrap();
        let ended = |n: &Note| matches!(n, Note::EpochEnd { .. });
        run.until("the connection's end", |r| {
            r.link.borrow().notes().iter().any(ended)
        })
        .await;
        // The pacing's one attempt a minute: the next opens a minute after the first.
        tokio::time::advance(Duration::from_secs(60)).await;
        // The orders each resync showed, in the order it showed them.
        let resyncs = |r: &Run<'_>| -> Vec<Vec<Option<CidMatch>>> {
            let link = r.link.borrow();
            let shown = link.notes().iter().filter_map(|n| match n {
                Note::Resynced { snapshot, .. } => {
                    Some(snapshot.orders.iter().map(|o| o.cid).collect())
                }
                _ => None,
            });
            shown.collect()
        };
        run.until("the second resync applied", |r| resyncs(r).len() == 2)
            .await;
        let (ours_bid, ours_ask) = (Some(CidMatch::Ours(bid)), Some(CidMatch::Ours(ask)));
        assert_eq!(resyncs(run)[1], [ours_ask, ours_bid]);
        // The Safety cancels' answers, one for each order.
        let refusals = |r: &Run<'_>| {
            let link = r.link.borrow();
            let safety: Vec<RpcId> = [bid, ask]
                .iter()
                .filter_map(|cid| link.safety_cancel_of(*cid))
                .collect();
            let of = |n: &&Note| matches!(n, Note::Outcome { rpc, ours: true, .. } if safety.contains(rpc));
            link.notes().iter().filter(of).count()
        };
        run.until("both Safety cancels refused", |r| refusals(r) == 2)
            .await;
        // Learnt again by the resync and as unprotected, each keeps when it was first learnt.
        assert_eq!(run.live(), [bid, ask]);

        run.link.borrow_mut().reg().disarm(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        let refused = Why::Refused(CancelAllRefusal::NoExclusiveLease);
        assert_eq!(round.how, How::Explicit(refused));
        assert_eq!(round.sent.len(), 1);
        assert_eq!(round.sent[0].names, [bid, ask]);
        let rpc = round.sent[0].rpc;
        run.until("both ended", |r| r.live().is_empty() && r.answered(rpc))
            .await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert_eq!(run.kinds(), ["place", "place", "cancels", "cancels"]);
    });
}

#[test]
fn an_order_whose_cancel_came_back_unknown_is_cancelled_again_by_name() {
    // The bid's own cancel, the consumer's, is never answered.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let cancel = run.cancel(bid);
        run.until("the bid's cancel read", |r| r.kinds().len() == 2)
            .await;
        // At the toy's request deadline the session reports its fate Unknown: the cancel stays
        // in flight on the bid, and fbc-oms puts the bid on the Unknown ladder.
        tokio::time::advance(RPC_TIMEOUT).await;
        run.until("the cancel's fate unknown", |r| r.answered(cancel))
            .await;
        {
            let link = run.link.borrow();
            let record = link.registry().get(bid).unwrap();
            assert!(record.unknown_since().is_some());
            assert_ne!(record.intent(), Intent::None);
        }

        // Explicit cancels name it again.
        run.link.borrow_mut().reg().disarm(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        let refused = Why::Refused(CancelAllRefusal::NoExclusiveLease);
        assert_eq!(round.how, How::Explicit(refused));
        assert_eq!(round.sent.len(), 1);
        assert_eq!(round.sent[0].names, [bid]);
        let rpc = round.sent[0].rpc;
        run.until("the bid ended", |r| r.live().is_empty() && r.answered(rpc))
            .await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert_eq!(run.kinds(), ["place", "cancel", "cancels"]);
    });
}

#[test]
fn before_places_a_refusal_of_a_cancel_not_the_sweeps_fails_nothing() {
    // The bid's own cancel, the consumer's, and the sweep's cancel-all are held.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent]),
        cancel_alls: VecDeque::from([Answer::Silent]),
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let bid = run.place(Side::Buy, 100).await;
        let cancel = run.cancel(bid);
        run.until("the bid's cancel read", |r| r.kinds().len() == 2)
            .await;
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.how, How::CancelAll);
        let all = round.sent[0].rpc;
        run.until("the cancel-all read", |r| r.kinds().len() == 3)
            .await;
        // The consumer's cancel refused for a reason neither its rate nor its order's end: the
        // consumer judges it, and the sweep waits on.
        run.push(format!("item|rpc={}|i=0|res=rej|code=9999", cancel.0))
            .await;
        run.until("the refusal noted", |r| r.answered(cancel)).await;
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        assert!(polled.resent.is_none());
        assert_eq!(swept.sweep.outcome(cancel), None);

        // The cancel-all answered and the bid ended: the sweep ends, the market taking places.
        run.push(format!("item|rpc={}|i=0|res=ok", all.0)).await;
        run.end_order(0, "canceled|why=requested").await;
        run.until("the bid ended", |r| r.live().is_empty() && r.answered(all))
            .await;
        assert!(run.orders.may_place_on(INST_A));
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
        assert_eq!(swept.sweep.rpcs().to_vec(), [all]);
        assert_eq!(run.kinds(), ["place", "cancel", "cancelall"]);
    });
}

#[test]
fn a_cancel_awaiting_its_orders_acknowledgement_is_sent_as_due_not_in_the_round() {
    play(awaiting_caps(), Venue::default(), async |run| {
        run.started().await;
        // Another program cancels an order of ours: the sweep's cancels are explicit, and
        // fbc-oms builds no cancel of everything of its own beside them.
        run.place(Side::Sell, 110).await;
        run.end_order(0, "canceled|why=requested").await;
        run.until("the cancel not ours noted", |r| {
            r.link.borrow().cancels_not_ours_seen()
        })
        .await;
        // The consumer's cancel of the bid waits for its acknowledgement, which then lands.
        lock(&run.venue).hold_acks = true;
        let bid = run.place(Side::Buy, 100).await;
        let choice = {
            let mut link = run.link.borrow_mut();
            link.reg()
                .cancellable(bid)
                .unwrap()
                .cancel(&awaiting_caps())
        };
        assert!(matches!(choice, CancelChoice::AwaitAck));
        run.ack(1).await;
        run.until("the bid acknowledged", |r| r.state(bid) == OrdState::Open)
            .await;
        assert!(
            run.link
                .borrow()
                .registry()
                .get(bid)
                .unwrap()
                .cancel_awaits_ack()
        );

        // The explicit cancels leave it to the poll, which sends it as due.
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.how, How::Explicit(Why::NotOursSeen));
        assert!(round.sent.is_empty());
        let polled = run.poll(&mut stop);
        assert_eq!(polled.due.len(), 1);
        assert_eq!(polled.due[0].names, [bid]);
        let due = polled.due[0].rpc;
        run.until("the bid ended", |r| r.live().is_empty() && r.answered(due))
            .await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert_eq!(stop.sweep.rpcs().to_vec(), [due]);
        assert_eq!(run.kinds(), ["place", "place", "cancel"]);
    });
}

#[test]
fn a_sweep_sends_only_its_own_markets_due_cancels_and_another_markets_cancel_holds_nothing() {
    // Every place's acknowledgement is held. The first cancel is answered and ends its order;
    // the second is left unanswered.
    let venue = Venue {
        hold_acks: true,
        cancels: VecDeque::from([Answer::Cancel, Answer::Silent]),
        two_markets: true,
        ..Venue::default()
    };
    let singles = OrderCaps {
        batch_cancel: None,
        ..awaiting_caps()
    };
    play(awaiting_caps(), venue, async |run| {
        run.started().await;
        run.started_b();
        let ours = run.place(Side::Buy, 100).await;
        let other = run.place_on(INST_B, Side::Buy, 100).await;
        // The consumer's cancel of its order on INST_B waits for that order's acknowledgement.
        let choice = {
            let mut link = run.link.borrow_mut();
            let cancellable = link.reg().cancellable(other).unwrap();
            cancellable.cancel(&awaiting_caps())
        };
        assert!(matches!(choice, CancelChoice::AwaitAck));
        // Stop on INST_A: the instrument cancel-all, the order's own cancel waiting for its
        // acknowledgement.
        run.link.borrow_mut().reg().kill(INST_A);
        let (mut stop, round) = run.start(SweepEnd::AtStop);
        assert_eq!(round.how, How::CancelAll);
        assert_eq!(round.sent.len(), 1);
        let all = round.sent[0].rpc;
        run.until("the cancel-all answered", |r| r.answered(all))
            .await;
        assert!(run.poll(&mut stop).due.is_empty());

        // Both acknowledgements land: each order's cancel is due in the registry.
        run.ack(0).await;
        run.ack(1).await;
        run.until("both acknowledged", |r| {
            r.state(ours) == OrdState::Open && r.state(other) == OrdState::Open
        })
        .await;
        let due = run.link.borrow().registry().cancels_due(&awaiting_caps());
        assert_eq!(due, [ours, other]);

        // The sweep sends its own market's alone: the other market's stays due, for the consumer
        // or that market's own sweep, and the venue reads no cancel of it.
        let polled = run.poll(&mut stop);
        assert!(polled.due_failed.is_empty());
        assert_eq!(polled.due.len(), 1);
        assert_eq!(polled.due[0].names, [ours]);
        let due = run.link.borrow().registry().cancels_due(&awaiting_caps());
        assert_eq!(due, [other]);
        let mine = polled.due[0].rpc;
        run.until("the order ended", |r| {
            r.live().is_empty() && r.answered(mine)
        })
        .await;
        assert_eq!(run.kinds(), ["place", "place", "cancelall", "cancel"]);

        // The consumer sends the other market's cancel, and the venue leaves it unanswered: the
        // sweep ends all the same, every order of ours on its market ended and every request it
        // sent answered.
        let theirs = run.submit(|reg| reg.cancel_many(&[other], &singles).commands.remove(0));
        run.until("the other market's cancel read", |r| r.kinds().len() == 5)
            .await;
        assert_eq!(run.poll(&mut stop).state, SweepState::Ended);
        assert_eq!(stop.sweep.rpcs().to_vec(), [all, mine]);
        assert!(!run.answered(theirs));
        assert_eq!(run.link.borrow().live_on(INST_B), [other]);
    });
}

#[test]
fn a_rate_refusal_makes_the_resend_due_only_for_a_cancel_on_the_sweeps_market() {
    // Every cancel and cancel-all is held, but the last cancel-all, which ends the order of
    // ours resting on its market.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent, Answer::Silent]),
        cancel_alls: VecDeque::from([Answer::Silent; 4]),
        two_markets: true,
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        run.started_b();
        let ours = run.place(Side::Buy, 100).await;
        let other = run.place_on(INST_B, Side::Buy, 100).await;
        let (mut swept, round) = run.start(SweepEnd::BeforePlaces);
        assert_eq!(round.how, How::CancelAll);
        let all = round.sent[0].rpc;
        run.until("the cancel-all read", |r| r.kinds().len() == 3)
            .await;

        // The consumer's cancel of its order on INST_B and its instrument cancel-all of INST_B,
        // each refused for its rate: neither is a cancel on the sweep's market, so nothing is
        // due, then or once the hold passed.
        let cancel_b = run.cancel(other);
        let all_b = run.submit(|reg| cancel_all_of(reg, INST_B));
        run.until("both read", |r| r.kinds().len() == 5).await;
        for rpc in [cancel_b, all_b] {
            run.push(format!("item|rpc={}|i=0|res=rej|code=1006", rpc.0))
                .await;
            run.until("the refusal noted", |r| r.answered(rpc)).await;
        }
        let at = run.now();
        let polled = run.poll(&mut swept);
        assert_eq!(run.hold.until(), Some(MonoNs(at.0 + ns(WAIT))));
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        assert!(polled.resent.is_none());
        tokio::time::advance(WAIT).await;
        let polled = run.poll(&mut swept);
        assert_eq!(polled.state, SweepState::Waiting { resend_at: None });
        assert!(polled.resent.is_none());
        let read = ["place", "place", "cancelall", "cancel", "cancelall"];
        assert_eq!(run.kinds(), read);

        // The consumer's cancel of its order on INST_A refused for its rate: the cancel of
        // everything is due again at the consumer's wait, and sent then.
        let cancel_a = run.cancel(ours);
        run.until("the cancel read", |r| r.kinds().len() == 6).await;
        run.push(format!("item|rpc={}|i=0|res=rej|code=1006", cancel_a.0))
            .await;
        run.until("the refusal noted", |r| r.answered(cancel_a))
            .await;
        let at = run.now();
        let resend_at = Some(MonoNs(at.0 + ns(WAIT)));
        assert_eq!(
            run.poll(&mut swept).state,
            SweepState::Waiting { resend_at }
        );
        tokio::time::advance(WAIT).await;
        let resent = run.poll(&mut swept).resent.expect("sent again");
        assert_eq!(resent.how, How::CancelAll);
        let again = resent.sent[0].rpc;
        run.until("the resend read", |r| r.kinds().len() == 7).await;

        // The consumer's instrument cancel-all of INST_A, not the sweep's, refused for its rate:
        // due again, and sent once the hold passed. That one ends the order.
        let all_a = run.submit(|reg| cancel_all_of(reg, INST_A));
        run.until("the cancel-all read", |r| r.kinds().len() == 8)
            .await;
        run.push(format!("item|rpc={}|i=0|res=rej|code=1006", all_a.0))
            .await;
        run.until("the refusal noted", |r| r.answered(all_a)).await;
        let at = run.now();
        let resend_at = Some(MonoNs(at.0 + ns(WAIT)));
        assert_eq!(
            run.poll(&mut swept).state,
            SweepState::Waiting { resend_at }
        );
        tokio::time::advance(WAIT).await;
        let resent = run.poll(&mut swept).resent.expect("sent again");
        assert_eq!(resent.how, How::CancelAll);
        let last = resent.sent[0].rpc;
        run.until("the order ended", |r| {
            r.live().is_empty() && r.answered(last)
        })
        .await;
        assert_eq!(run.poll(&mut swept).state, SweepState::Ended);
        assert_eq!(swept.sweep.rpcs().to_vec(), [all, again, last]);
        assert_eq!(run.link.borrow().live_on(INST_B), [other]);
        let syms = lock(&run.venue).cancel_all_syms.clone();
        assert_eq!(syms, [SYMBOL_A, SYMBOL_B, SYMBOL_A, SYMBOL_A, SYMBOL_A]);
    });
}

#[test]
fn the_link_says_on_which_market_each_cancel_it_holds_cancels() {
    // Nothing a cancel names ends by it: each is left unanswered.
    let venue = Venue {
        cancels: VecDeque::from([Answer::Silent, Answer::Silent]),
        cancel_alls: VecDeque::from([Answer::Silent]),
        two_markets: true,
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        run.started_b();
        let ours = run.place(Side::Buy, 100).await;
        let other = run.mint.mint().unwrap();
        let place = run.place_as_on(INST_B, other, Side::Buy, 100).await;
        // A cancel of each, and the consumer's instrument cancel-all of INST_B.
        let cancel_a = run.cancel(ours);
        let cancel_b = run.cancel(other);
        let all_b = run.submit(|reg| cancel_all_of(reg, INST_B));
        run.until("every request read", |r| r.kinds().len() == 5)
            .await;
        assert_eq!(
            run.kinds(),
            ["place", "place", "cancel", "cancel", "cancelall"]
        );
        // The venue ends the order on INST_B itself: its cancel still names it.
        run.end_order(1, "expired").await;
        run.until("the order ended", |r| r.state(other).is_terminal())
            .await;

        let mut link = run.link.borrow_mut();
        assert!(link.is_cancel_on(cancel_a, INST_A));
        assert!(!link.is_cancel_on(cancel_a, INST_B));
        for rpc in [cancel_b, all_b] {
            assert!(link.is_cancel_on(rpc, INST_B), "request {}", rpc.0);
            assert!(!link.is_cancel_on(rpc, INST_A), "request {}", rpc.0);
        }
        assert!(!link.is_cancel_on(place, INST_A));
        assert!(!link.is_cancel_on(place, INST_B));
        // Released, none is a cancel on either market any more.
        for rpc in [cancel_a, cancel_b, all_b] {
            link.release(rpc);
            for inst in [INST_A, INST_B] {
                assert!(!link.is_cancel_on(rpc, inst), "request {} released", rpc.0);
            }
        }
    });
}
