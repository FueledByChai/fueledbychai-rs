//! The link times each submission it sends on its session's shard clock (FBC-y7lq, decision
//! 0155 item 3): the conformance toy's order entry on an fbc-runtime `ExecSession` against the
//! conformance stub, on a paused runtime whose clock moves only when the test moves it. A place
//! sent after the clock moved on is timed at the session's `ExecOrders::now` as the session
//! reports it sent, on the clock its inputs are stamped on, whether the link was built with the
//! session's clock or ten seconds after it; before it is attached or submits, the link reads no
//! clock. An `ExecOrders` on another shard clock is noted as a problem and read from then on,
//! and a cancel submitted through the session's own after it still goes to the venue, timed on
//! the session's clock again.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_conformance::toy::{EXEC_URL_KEY, INST_A, OWN_NS, SYMBOL_A, TOY_TOKEN, ToyFactory};
use fbc_conformance::{Frame, HttpRouter, Responded, Responder, Step, StubServer, WsScript};
use fbc_core::{
    AccountKey, AccountLease, Channel, CidMint, ClientOrderId, ConnKey, ConnState, Envelope,
    ExecEvent, Lots, MarketLease, MonoNs, NamespaceLease, NewOrder, NonceBlock, NonceSource,
    OrderCaps, OrderKind, RpcId, Secrets, Side, SubmitHandle, Ticks, Tif, VenueConfig,
    VenueFactory, VenueOrderSnapshot, WallNs, dispatch_market_data,
};
use fbc_live::link::{Link, LinkHandler, Note};
use fbc_oms::{
    CancelChoice, FillLedger, Intent, LadderConfig, LeaseKeys, Leases, LedgerConfig,
    MarketCapsConfig, PreTradeCaps, Registry,
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

/// How far the paused clock moves on between the session's authentication and the place.
const LATER: Duration = Duration::from_secs(3);

/// The steps of the stub's opening: the connection accepted, the authentication, the
/// cancel-on-disconnect arm and a resync of a flat account answered as the toy's setup answers
/// them, then a barrier, so the link has applied the resync once it has passed.
const OPENING: usize = 5;

/// The toy's order caps.
fn order_caps() -> OrderCaps {
    fbc_conformance::toy::caps().exec.unwrap().order
}

/// A directory of the test's own for its leases, removed when it is dropped.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Dir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("fbc-live-clock-test-{}-{n}", std::process::id()));
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

/// The text of a frame the toy sent: it sends no other kind.
fn text(frame: &Frame) -> &str {
    match frame {
        Frame::Text(text) => text,
        Frame::Binary(_) => panic!("the toy sends text frames only"),
    }
}

/// The value of `key` in the toy record `frame` (`kind|key=value|...`).
fn field(frame: &Frame, key: &str) -> String {
    let head = text(frame).lines().next().unwrap_or_default();
    let mut fields = head.split('|').skip(1).filter_map(|kv| kv.split_once('='));
    let found = fields.find_map(|(k, v)| (k == key).then(|| v.to_owned()));
    found.unwrap_or_else(|| panic!("no {key} in {head}"))
}

/// A responder answering a request of `kind` with the records `with` computes from it.
fn answer(kind: &'static str, with: fn(&Frame) -> Vec<String>) -> Responder {
    Responder::new(move |frame| -> Responded {
        let read = text(frame).split('|').next().unwrap_or_default();
        if read != kind {
            return Err(format!("expected {kind}, read {read}"));
        }
        Ok(with(frame).into_iter().map(Frame::Text).collect())
    })
}

/// The stub's script: the opening ([`OPENING`]), then `reads` requests read and left
/// unanswered, each a wait point once read.
fn script(reads: usize) -> WsScript {
    let respond = |with| Step::Respond { conn: 0, with };
    let mut steps = vec![
        Step::Accept,
        respond(answer("auth", |_| {
            vec![format!("auth|ok=1|token={TOY_TOKEN}")]
        })),
        respond(answer("cod", |r| {
            vec![format!("item|rpc={}|i=0|res=ok", field(r, "rpc"))]
        })),
        respond(answer("resync", |r| {
            vec![
                format!("rsbegin|wm={}", field(r, "ts")),
                format!("rspos|sym={SYMBOL_A}|qty=0"),
                "rsend".into(),
            ]
        })),
        Step::Barrier { conn: 0 },
    ];
    steps.extend((0..reads).map(|_| Step::Read { conn: 0 }));
    WsScript::new(steps)
}

/// The toy's order entry for `ACCT` at `url`, stamping on `clock`, handing its events to
/// `handler`: one connection attempt, and deadlines no test here reaches.
fn session<H: ExecHandler>(
    url: &str,
    clock: &IngestClock,
    handler: H,
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
        outbound: ReservedBound::new(64, 64).unwrap(),
        in_flight: InFlight::new(ReservedBound::new(64, 64).unwrap()),
    };
    ExecSession::new(config, handler).unwrap()
}

/// A link over `reg` under the toy's order caps.
fn link_over(reg: Registry) -> Link {
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
    Link::new(reg, ledger, order_caps(), ladder)
}

/// A registry for `ACCT` on the toy, `INST_A` its one market under caps of 10 lots, its market
/// and account lease names "toy".
fn registry() -> Registry {
    let toy = fbc_conformance::toy::caps();
    let symbol = dispatch_market_data(&toy, |s| s.venue_symbol(SYMBOL_A)).unwrap();
    let keys = LeaseKeys::new("toy", "test-acct", &order_caps()).with_market(INST_A, symbol);
    let config = MarketCapsConfig {
        inventory: Lots::new(10),
        resting: Lots::new(10),
    };
    let market = PreTradeCaps::new().with_market(INST_A, config).unwrap();
    Registry::with_caps(market).with_lease_keys(keys)
}

/// What the session's handler saw of the session's clock besides what it handed the link.
#[derive(Default)]
struct Seen {
    /// The stamp of the input reporting the connection authenticated.
    authenticated: Option<MonoNs>,
    /// For each submission reported, the session's `ExecOrders::now` just before the link was
    /// handed it, and the wall time just after.
    reported: Vec<(RpcId, (MonoNs, WallNs), WallNs)>,
}

/// The session's handler: each call handed to the link, as `LinkHandler` does, with what the
/// test reads of the session's clock as it reports.
struct Timed {
    link: Rc<RefCell<Link>>,
    orders: Rc<RefCell<Option<ExecOrders>>>,
    seen: Rc<RefCell<Seen>>,
}

impl ExecHandler for Timed {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        if let ExecEvent::Conn {
            state: ConnState::Authenticated,
            ..
        } = env.body
        {
            self.seen.borrow_mut().authenticated = Some(env.stamp.recv_mono);
        }
        self.link.borrow_mut().on_exec(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.link.borrow_mut().on_epoch_end(key);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let now = || self.orders.borrow().as_ref().unwrap().now();
        let (rpc, before) = (handle.rpc, now());
        self.link.borrow_mut().on_submitted(handle);
        let after = now().1;
        self.seen.borrow_mut().reported.push((rpc, before, after));
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.link.borrow_mut().on_unprotected(orders);
    }
}

/// What a scenario drives: the link, the session's orders, the stub and what the handler saw.
struct Run<'a> {
    link: Rc<RefCell<Link>>,
    orders: ExecOrders,
    server: &'a StubServer,
    seen: Rc<RefCell<Seen>>,
    mint: CidMint,
    dir: &'a Dir,
}

impl Run<'_> {
    /// Lets the session and the stub run, the paused clock unmoved, until `done` holds; the test
    /// fails, naming `what`, once the watchdog's wall-clock time has passed.
    #[expect(
        clippy::disallowed_methods,
        reason = "the test's watchdog reads the wall clock; nothing the link times does"
    )]
    async fn until(&self, what: &str, done: impl Fn(&Run<'_>) -> bool) {
        let give_up = std::time::Instant::now() + WATCHDOG;
        while !done(self) {
            assert!(std::time::Instant::now() < give_up, "never {what}");
            tokio::task::yield_now().await;
        }
    }

    /// Waits until the session's epoch takes places and the link has applied its resync, then
    /// gives the owner's Start on `INST_A`.
    async fn started(&mut self) {
        self.until("ready", |r| {
            let resynced = |n: &Note| matches!(n, Note::Resynced { .. });
            r.orders.may_place() && r.link.borrow().notes().iter().any(resynced)
        })
        .await;
        let toy = fbc_conformance::toy::caps();
        let symbol = dispatch_market_data(&toy, |s| s.venue_symbol(SYMBOL_A)).unwrap();
        let market = MarketLease::acquire(&self.dir.0, "toy", "test-acct", &symbol).unwrap();
        let account = AccountLease::acquire(&self.dir.0, "toy", "test-acct").unwrap();
        let leases = Leases::market(market).with_account(account);
        self.link.borrow_mut().reg().start(INST_A, leases).unwrap();
    }

    /// One post-only buy of a lot at 100 ticks on `INST_A`, authorized and submitted through the
    /// link and the session's orders: its client id and request id.
    fn place(&mut self) -> (ClientOrderId, RpcId) {
        let cid = self.mint.mint().unwrap();
        let new = NewOrder {
            cid,
            inst: INST_A,
            side: Side::Buy,
            kind: OrderKind::Limit { px: Ticks(100) },
            qty: Lots::new(1).unwrap(),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let mut link = self.link.borrow_mut();
        let cmd = link.reg().place(new).unwrap();
        let auth = link.reg().authorize(ACCT, cmd).unwrap();
        (cid, link.submit(&self.orders, auth).unwrap())
    }

    /// The cancel of `cid` through its cancel permit, authorized and submitted through the link
    /// and the session's orders: its request id.
    fn cancel(&mut self, cid: ClientOrderId) -> RpcId {
        let mut link = self.link.borrow_mut();
        let choice = link.reg().cancellable(cid).unwrap().cancel(&order_caps());
        let CancelChoice::Send(cmd) = choice else {
            panic!("the toy cancels before its acknowledgement, by our client id");
        };
        let auth = link.reg().authorize(ACCT, cmd).unwrap();
        link.submit(&self.orders, auth).unwrap()
    }

    /// Waits until the stub has read the `n`th request after the opening.
    async fn read(&self, n: usize) {
        self.until("read", |r| r.server.played() >= OPENING + n)
            .await;
    }

    /// The session's report of request `rpc`: the session's time before the link was handed
    /// it, and the wall time after.
    fn reported(&self, rpc: RpcId) -> ((MonoNs, WallNs), WallNs) {
        let seen = self.seen.borrow();
        let found = seen.reported.iter().find(|(r, ..)| *r == rpc);
        let (_, before, after) = found.expect("the session reported the request");
        (*before, *after)
    }

    /// The problems the link noted, in order.
    fn problems(&self) -> Vec<String> {
        let link = self.link.borrow();
        let problems = link.notes().iter().filter_map(|n| match n {
            Note::Problem(what) => Some(what.clone()),
            _ => None,
        });
        problems.collect()
    }
}

/// Runs `scenario` against the toy's order entry and the stub playing [`script`] with `reads`
/// requests read, on a paused runtime nothing moves but the scenario: the session's clock is
/// built at the runtime's start, and the link `built_after` it, attached to the session before
/// it runs.
fn play(built_after: Duration, reads: usize, scenario: impl AsyncFnOnce(&mut Run<'_>)) {
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
        tokio::time::advance(built_after).await;
        let link = Rc::new(RefCell::new(link_over(registry())));
        assert_eq!(
            link.borrow().now(),
            None,
            "a link attached to no session reads no clock"
        );
        let server = StubServer::start(script(reads), HttpRouter::new())
            .await
            .unwrap();
        let (orders, seen) = (Rc::new(RefCell::new(None)), Rc::default());
        let handler = Timed {
            link: Rc::clone(&link),
            orders: Rc::clone(&orders),
            seen: Rc::clone(&seen),
        };
        let (mut session, control) = session(&server.ws_url("/exec"), &clock, handler);
        *orders.borrow_mut() = Some(session.orders());
        link.borrow_mut().attach(session.orders(), ACCT);
        let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
        let mut run = Run {
            link,
            orders: session.orders(),
            server: &server,
            seen,
            mint: CidMint::new(lease, 0, 0, WallNs(1_000_000_000)),
            dir: &dir,
        };
        let driven = async {
            scenario(&mut run).await;
            drop(control);
        };
        let (ran, ()) = tokio::join!(session.run(), driven);
        drop(thaw);
        ran.unwrap();
        server.finished().await.unwrap();
    });
}

/// The place `run` sends `LATER` after the session authenticated: its client id and request
/// id, once the stub read it.
async fn place_later(run: &mut Run<'_>) -> (ClientOrderId, RpcId) {
    run.started().await;
    tokio::time::advance(LATER).await;
    let placed = run.place();
    run.read(1).await;
    placed
}

/// Checks that `run`'s place `cid`, request `rpc`, was timed at the session's `ExecOrders::now`
/// as the session reported it, `LATER` after the session's authentication on its stamps.
fn timed_on_the_sessions_clock(run: &Run<'_>, cid: ClientOrderId, rpc: RpcId) {
    let ((mono, wall), after) = run.reported(rpc);
    let link = run.link.borrow();
    let (sent_mono, sent_wall) = link.registry().get(cid).unwrap().sent_at().unwrap();
    assert_eq!(
        sent_mono, mono,
        "the send time is the session's now as it reports"
    );
    assert!(
        wall <= sent_wall && sent_wall <= after,
        "and so is its wall time"
    );
    let authenticated = run.seen.borrow().authenticated.unwrap();
    let later = u64::try_from(LATER.as_nanos()).unwrap();
    assert_eq!(
        sent_mono,
        MonoNs(authenticated.0 + later),
        "on the stamps' clock"
    );
    assert_eq!(link.now().map(|n| n.0), Some(run.orders.now().0));
}

#[test]
fn a_send_is_timed_on_the_sessions_shard_clock_at_its_stamps() {
    play(Duration::ZERO, 1, async |run| {
        let (cid, rpc) = place_later(run).await;
        timed_on_the_sessions_clock(run, cid, rpc);
        assert_eq!(run.problems(), Vec::<String>::new());
    });
}

#[test]
fn a_link_built_ten_seconds_after_the_sessions_clock_still_times_its_sends_on_the_stamps() {
    play(Duration::from_secs(10), 1, async |run| {
        let (cid, rpc) = place_later(run).await;
        timed_on_the_sessions_clock(run, cid, rpc);
        // The session's clock started ten seconds before the link was built.
        let authenticated = run.seen.borrow().authenticated.unwrap();
        assert_eq!(authenticated, MonoNs(10_000_000_000));
    });
}

#[test]
fn a_session_on_another_clock_is_noted_and_a_cancel_after_it_still_goes() {
    play(Duration::ZERO, 2, async |run| {
        let (cid, _) = place_later(run).await;
        assert_eq!(run.problems(), Vec::<String>::new());

        // The link attached to another session, built on a clock of its own a moment later and
        // never run: noted, and the link reads that clock from then on.
        tokio::time::advance(LATER).await;
        let other = IngestClock::new();
        let elsewhere = LinkHandler {
            link: Rc::new(RefCell::new(link_over(Registry::new()))),
            wake: Box::new(|| {}),
        };
        let (other_session, _other_control) = session("ws://127.0.0.1:9/", &other, elsewhere);
        let other_orders = other_session.orders();
        assert!(!other_orders.shares_clock(&run.orders));
        run.link.borrow_mut().attach(other_orders.clone(), ACCT);
        assert_eq!(run.problems().len(), 1);
        assert!(run.problems()[0].contains("another shard clock"));
        assert_eq!(run.link.borrow().now().map(|n| n.0), Some(MonoNs(0)));

        // The cancel, submitted through the running session's orders: noted again, as the
        // link's clock was the other's, and still sent to the venue, timed on the session's
        // clock again.
        let rpc = run.cancel(cid);
        assert_eq!(run.problems().len(), 2);
        run.read(2).await;
        let received = run.server.connections()[0].received.clone();
        let last = received.last().expect("the stub read the cancel");
        assert_eq!(text(last).split('|').next(), Some("cancel"));
        assert_eq!(field(last, "rpc"), rpc.0.to_string());
        let link = run.link.borrow();
        let sent = link
            .notes()
            .iter()
            .any(|n| matches!(n, Note::Submitted { rpc: r, sent: Ok(()) } if *r == rpc));
        assert!(sent, "the session reported the cancel sent");
        let ((mono, _), _) = run.reported(rpc);
        let since = match link.registry().get(cid).unwrap().intent() {
            Intent::PendingCancel { rpc: r, since } if r == rpc => since,
            other => panic!("no cancel in flight: {other:?}"),
        };
        assert_eq!(since, mono);
        let authenticated = run.seen.borrow().authenticated.unwrap();
        let both = u64::try_from((LATER * 2).as_nanos()).unwrap();
        assert_eq!(since, MonoNs(authenticated.0 + both));
    });
}
