//! The link's order queries and fbc-oms's Unknown ladder through it (FBC-2cv0, decisions 0005's
//! I5 and I9, 0080, 0081 and 0109): the conformance toy's order entry on an fbc-runtime
//! `ExecSession` against the conformance stub's serving script, a toy venue of the test's own,
//! on a paused runtime whose clock moves only when the test moves it, every order command
//! through an fbc-oms authorization and the session's `ExecOrders`.
//!
//! An unprotected order the registry holds ended (a resync lagging the stream shows it resting
//! after our cancel ended it) is queried, not cancelled, and its market is released once the
//! answer shows it ended, and held while the answer shows it open or does not come; one the
//! venue's caps declare no query reference for is noted and nothing is sent. An order gone
//! Unknown is queried by the ladder the consumer runs (`Link::ladder`) and resolved by the
//! answer; a tombstone due is sent once, by client id, and recorded, so its acceptance ends the
//! order. What a pass cannot send is noted with why, on a session that is never run.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use fbc_conformance::toy::{
    EXEC_URL_KEY, INST_A, OWN_NS, REST_URL_KEY, RPC_TIMEOUT, SYMBOL_A, TOY_TOKEN, ToyFactory,
};
use fbc_conformance::{Frame, HttpRouter, Pusher, Responded, Responder, StubServer, WsScript};
use fbc_core::{
    AccountKey, AccountLease, AckLevel, CancelReason, Channel, CidMatch, CidMint, ClientOrderId,
    ConnKey, Envelope, ExchNs, ExchTsKind, ExecEvent, ItemRef, Lots, MarketLease, MonoNs,
    NamespaceLease, NewOrder, NonceBlock, NonceSource, NotSentReason, OrderCaps, OrderKind,
    QueryAnswer, RefKind, RpcId, Secrets, Side, Stamp, SubmitHandle, SubmitOutcome, TagSet, Ticks,
    Tif, VenueConfig, VenueFactory, VenueOrderState, WallNs, dispatch_market_data,
};
use fbc_live::link::{
    LadderCommand, LadderNote, LadderPass, Link, LinkHandler, Note, Queried, QueryRead,
};
use fbc_oms::{
    FillLedger, LadderConfig, LadderResolution, LeaseKeys, Leases, LedgerConfig, MarketCapsConfig,
    OrdState, PreTradeCaps, Registry, TerminalKind,
};
use fbc_runtime::{
    Connector, ExecControl, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, InFlight,
    IngestClock, ProxyConfig, RateLimiter, ReconnectPacing, ReservedBound, ResyncDeadline, RpcIds,
    SafetyReserve, WriteStall,
};

static TOY: ToyFactory = ToyFactory;

/// The account the session trades and the registry authorizes for.
const ACCT: AccountKey = AccountKey::new(1);

/// How long, by the wall clock, a wait lasts before the test fails.
const WATCHDOG: Duration = Duration::from_secs(60);

/// The ladder's maximum time on it before a tombstone, as [`ladder_config`] sets it.
const MAX_UNKNOWN: Duration = Duration::from_secs(60);

/// The toy's order caps.
fn order_caps() -> OrderCaps {
    fbc_conformance::toy::caps().exec.unwrap().order
}

/// The consumer's ladder: intent timeout 5 s, settle 1 s, [`MAX_UNKNOWN`], two snapshots.
fn ladder_config() -> LadderConfig {
    LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        MAX_UNKNOWN,
        2,
    )
    .unwrap()
}

/// A directory of the test's own for its leases, removed when it is dropped.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Dir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("fbc-live-ladder-test-{}-{n}", std::process::id()));
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

/// The value of `key` in the toy record `line` (`kind|key=value|...`), if it has one.
fn get(line: &str, key: &str) -> Option<String> {
    let mut fields = line.split('|').skip(1).filter_map(|kv| kv.split_once('='));
    fields.find_map(|(k, v)| (k == key).then(|| v.to_owned()))
}

/// The value of `key` in the toy record `line`.
fn field(line: &str, key: &str) -> String {
    get(line, key).unwrap_or_else(|| panic!("no {key} in {line}"))
}

/// How the test's venue answers a query.
#[derive(Clone, Copy, Debug, Default)]
enum Answers {
    /// Not at all: the test pushes the answer it wants, or none.
    #[default]
    Held,
    /// From what the venue holds of the order it names: open while it rests, else canceled.
    FromState,
    /// The venue does not know the order.
    NotFound,
}

/// An order the test's venue holds.
struct Order {
    cid: String,
    vid: String,
    side: String,
    px: String,
    qty: String,
    nonce: String,
    resting: bool,
}

/// The test's toy venue, shared with the stub's responder.
#[derive(Default)]
struct Venue {
    seq: u64,
    orders: Vec<Order>,
    /// Places are left unanswered.
    hold_acks: bool,
    /// The resync shows every order placed as open, ended or not: a snapshot lagging the stream.
    stale: bool,
    queries: Answers,
    /// A cancel is answered with its acceptance alone, no order event.
    cancel_answer_only: bool,
    /// Each request's first line as read, in order.
    read: Vec<String>,
}

type Shared = Arc<Mutex<Venue>>;

fn lock(venue: &Shared) -> MutexGuard<'_, Venue> {
    venue.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Venue {
    /// The order `line` names, by its venue id, client id or placement nonce.
    fn named(&self, line: &str) -> Option<usize> {
        let by = |o: &Order| {
            get(line, "vid") == Some(o.vid.clone())
                || get(line, "cid") == Some(o.cid.clone())
                || get(line, "pnonce") == Some(o.nonce.clone())
        };
        self.orders.iter().position(by)
    }

    /// Order `idx`'s fields as a resync or a query answer reports it, in `st`.
    fn shown(&self, idx: usize, st: &str) -> String {
        let o = &self.orders[idx];
        format!(
            "cid={}|vid={}|sym={SYMBOL_A}|side={}|st={st}|px={}|qty={}|cum=0|po=1|ro=0",
            o.cid, o.vid, o.side, o.px, o.qty
        )
    }

    /// Order `idx` ended by request: its order event.
    fn end(&mut self, idx: usize) -> String {
        self.seq += 1;
        self.orders[idx].resting = false;
        let (shown, seq) = (self.shown(idx, "canceled|why=requested"), self.seq);
        format!("order|{shown}|seq={seq}")
    }
}

/// The test's venue's answer to `frame`.
fn respond(venue: &Shared, frame: &Frame) -> Responded {
    let Frame::Text(text) = frame else {
        return Err("the toy sends text frames only".to_owned());
    };
    let head = text.lines().next().unwrap_or_default();
    let kind = head.split('|').next().unwrap_or_default();
    let mut v = lock(venue);
    v.read.push(head.to_owned());
    let frames = match kind {
        "auth" => vec![format!("auth|ok=1|token={TOY_TOKEN}")],
        "cod" => vec![format!("item|rpc={}|i=0|res=ok", field(head, "rpc"))],
        "resync" => {
            let mut records = vec![format!("rsbegin|wm={}", field(head, "ts"))];
            for idx in 0..v.orders.len() {
                if v.orders[idx].resting || v.stale {
                    records.push(format!("rsorder|{}", v.shown(idx, "open")));
                }
            }
            records.push(format!("rspos|sym={SYMBOL_A}|qty=0"));
            records.push("rsend".to_owned());
            records
        }
        "place" => {
            let (rpc, vid) = (field(head, "rpc"), format!("V-{}", v.orders.len() + 1));
            v.orders.push(Order {
                cid: field(head, "cid"),
                vid: vid.clone(),
                side: field(head, "side"),
                px: field(head, "px"),
                qty: field(head, "qty"),
                nonce: field(head, "nonce"),
                resting: true,
            });
            let ack = format!("item|rpc={rpc}|i=0|res=ok|vid={vid}");
            if v.hold_acks { Vec::new() } else { vec![ack] }
        }
        "cancel" => {
            let rpc = field(head, "rpc");
            let idx = v
                .named(head)
                .ok_or_else(|| format!("no order named in {head}"))?;
            let mut frames = vec![format!("item|rpc={rpc}|i=0|res=ok")];
            if !v.cancel_answer_only {
                frames.push(v.end(idx));
            }
            frames
        }
        "cancels" => {
            let rpc = field(head, "rpc");
            let mut frames = Vec::new();
            for line in text.lines().skip(1) {
                let idx = v
                    .named(line)
                    .ok_or_else(|| format!("no order named in {line}"))?;
                frames.push(format!("item|rpc={rpc}|i={}|res=ok", field(line, "i")));
                if !v.cancel_answer_only {
                    frames.push(v.end(idx));
                }
            }
            frames
        }
        "query" => {
            let rpc = field(head, "rpc");
            match (v.queries, v.named(head)) {
                (Answers::Held, _) => Vec::new(),
                (Answers::FromState, Some(idx)) => {
                    let st = if v.orders[idx].resting {
                        "open"
                    } else {
                        "canceled|why=requested"
                    };
                    vec![format!("qres|rpc={rpc}|found=1|{}", v.shown(idx, st))]
                }
                (Answers::FromState | Answers::NotFound, _) => {
                    vec![format!("qres|rpc={rpc}|found=0")]
                }
            }
        }
        "ping" => Vec::new(),
        other => return Err(format!("the test's venue reads no {other}")),
    };
    Ok(frames.into_iter().map(Frame::Text).collect())
}

/// The toy's order entry for `ACCT` at `url`, stamping on `clock`, handing its events to
/// `handler`: one connection attempt a minute, and its REST origin `rest` (a loopback port
/// nothing listens on for a session that is never run).
fn session<H: ExecHandler>(
    url: &str,
    rest: Option<&str>,
    clock: &IngestClock,
    handler: H,
) -> (ExecSession<H>, ExecControl) {
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, url);
    if let Some(rest) = rest {
        cfg.insert(REST_URL_KEY, rest);
    }
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

/// A link over a registry for `ACCT` on the toy, `INST_A` its one market under caps of 10
/// lots, its lease names "toy", the link under `caps` and [`ladder_config`].
fn link_under(caps: OrderCaps) -> Link {
    let toy = fbc_conformance::toy::caps();
    let symbol = dispatch_market_data(&toy, |s| s.venue_symbol(SYMBOL_A)).unwrap();
    let keys = LeaseKeys::new("toy", "test-acct", &caps).with_market(INST_A, symbol);
    let config = MarketCapsConfig {
        inventory: Lots::new(10),
        resting: Lots::new(10),
    };
    let market = PreTradeCaps::new().with_market(INST_A, config).unwrap();
    let reg = Registry::with_caps(market).with_lease_keys(keys);
    let ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1_000,
        },
        WallNs(0),
    )
    .unwrap();
    Link::new(reg, ledger, caps, ladder_config())
}

/// A post-only buy of a lot at 100 ticks on `INST_A`, as our client id `cid`.
fn new_order(cid: ClientOrderId) -> NewOrder {
    NewOrder {
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
    }
}

/// Starts `INST_A` on `link`'s registry under leases in `dir`.
fn start(link: &mut Link, dir: &Dir) {
    let toy = fbc_conformance::toy::caps();
    let symbol = dispatch_market_data(&toy, |s| s.venue_symbol(SYMBOL_A)).unwrap();
    let market = MarketLease::acquire(&dir.0, "toy", "test-acct", &symbol).unwrap();
    let account = AccountLease::acquire(&dir.0, "toy", "test-acct").unwrap();
    let leases = Leases::market(market).with_account(account);
    link.reg().start(INST_A, leases).unwrap();
}

/// A query's answer as the link noted it: its order, request, purpose, read and resolution.
type Answered = (
    ClientOrderId,
    RpcId,
    Queried,
    QueryRead,
    Option<LadderResolution>,
);

/// What a scenario drives.
struct Run {
    link: Rc<RefCell<Link>>,
    orders: ExecOrders,
    pusher: Pusher,
    venue: Shared,
    mint: CidMint,
    dir: Dir,
}

impl Run {
    /// Lets the session and the stub run, the paused clock unmoved, until `done` holds.
    #[expect(
        clippy::disallowed_methods,
        reason = "the test's watchdog reads the wall clock; nothing the link times does"
    )]
    async fn until(&self, what: &str, done: impl Fn(&Run) -> bool) {
        let give_up = std::time::Instant::now() + WATCHDOG;
        while !done(self) {
            assert!(std::time::Instant::now() < give_up, "never {what}");
            tokio::task::yield_now().await;
        }
    }

    /// How many resyncs the link has applied.
    fn resyncs(&self) -> usize {
        let link = self.link.borrow();
        let resynced = |n: &&Note| matches!(n, Note::Resynced { .. });
        link.notes().iter().filter(resynced).count()
    }

    /// Waits for the session's first resync, then gives the owner's Start on `INST_A`.
    async fn started(&mut self) {
        self.until("ready", |r| r.orders.may_place() && r.resyncs() == 1)
            .await;
        start(&mut self.link.borrow_mut(), &self.dir);
    }

    /// Our order placed through the link: its client id, once the venue read it.
    async fn place(&mut self) -> ClientOrderId {
        let cid = self.mint.mint().unwrap();
        let placed = lock(&self.venue).orders.len() + 1;
        {
            let mut link = self.link.borrow_mut();
            let cmd = link.reg().place(new_order(cid)).unwrap();
            let auth = link.reg().authorize(ACCT, cmd).unwrap();
            link.submit(&self.orders, auth).unwrap();
        }
        self.until("the place read", |r| lock(&r.venue).orders.len() == placed)
            .await;
        cid
    }

    /// The registry's state of our order `cid`.
    fn state(&self, cid: ClientOrderId) -> OrdState {
        self.link.borrow().registry().get(cid).unwrap().state()
    }

    /// The session's shard clock's now, as the link reads it.
    fn now(&self) -> MonoNs {
        self.link.borrow().now().unwrap().0
    }

    /// One pass of the ladder through the link at the session's clock's now.
    fn ladder(&self) -> LadderPass {
        let now = self.now();
        self.link.borrow_mut().ladder(&self.orders, now)
    }

    /// The requests of `kind` the venue read, in order.
    fn read(&self, kind: &str) -> Vec<String> {
        let read = lock(&self.venue).read.clone();
        let of = |l: &String| l.split('|').next() == Some(kind);
        read.into_iter().filter(of).collect()
    }

    /// The answers to queries the link noted, in order.
    fn answers(&self) -> Vec<Answered> {
        let link = self.link.borrow();
        let answers = link.ladder_notes().iter().filter_map(|n| match n {
            LadderNote::QueryAnswer {
                cid,
                rpc,
                of,
                read,
                resolution,
            } => Some((*cid, *rpc, *of, read.clone(), *resolution)),
            _ => None,
        });
        answers.collect()
    }

    /// Sends `text` on the session's second connection, the one after the drop, as the venue
    /// would.
    async fn push(&self, text: String) {
        self.push_on(1, text).await;
    }

    /// Sends `text` on the session's connection `conn`, counted from 0, as the venue would.
    async fn push_on(&self, conn: usize, text: String) {
        self.pusher.push(conn, Frame::Text(text)).await.unwrap();
    }

    /// Our acknowledged order `cid` cancelled through the link, by a single cancel naming its
    /// venue id (the toy's batch cancel is another frame), once it has ended.
    async fn cancelled(&mut self, cid: ClientOrderId) {
        self.until("the place acknowledged", |r| r.state(cid) == OrdState::Open)
            .await;
        {
            let mut link = self.link.borrow_mut();
            let single = OrderCaps {
                batch_cancel: None,
                ..order_caps()
            };
            let cmd = link.reg().cancel_many(&[cid], &single).commands.remove(0);
            let auth = link.reg().authorize(ACCT, cmd).unwrap();
            link.submit(&self.orders, auth).unwrap();
        }
        self.until("the order ended", |r| r.state(cid).is_terminal())
            .await;
    }

    /// Where the link noted the orders the session told unprotected, once it has.
    async fn told(&self) -> usize {
        let told = |r: &Run| {
            let unprotected = |n: &Note| matches!(n, Note::Unprotected { .. });
            r.link.borrow().notes().iter().position(unprotected)
        };
        self.until("the orders told unprotected", |r| told(r).is_some())
            .await;
        told(self).unwrap()
    }

    /// The connection drops, and the next epoch's resync lags the stream, showing every order
    /// placed as resting: where the link noted the orders the session then told unprotected.
    async fn reconnected_stale(&mut self) -> usize {
        lock(&self.venue).stale = true;
        self.pusher.close(0).await.unwrap();
        let ended = |n: &Note| matches!(n, Note::EpochEnd { .. });
        self.until("the connection's end", |r| {
            r.link.borrow().notes().iter().any(ended)
        })
        .await;
        // The pacing's one attempt a minute: the next opens a minute after the first.
        tokio::time::advance(Duration::from_secs(60)).await;
        self.told().await
    }

    /// Our order placed and acknowledged, then cancelled through the link and ended; then the
    /// connection drops and the next epoch's resync, lagging, shows it resting, so the session
    /// tells it unprotected while the registry holds it ended. Its client id, and the link's
    /// [`Note::Unprotected`] and the notes after it, from when it was told.
    async fn ended_then_unprotected(&mut self) -> (ClientOrderId, usize) {
        self.started().await;
        let cid = self.place().await;
        self.cancelled(cid).await;
        let at = self.reconnected_stale().await;
        let link = self.link.borrow();
        let Note::Unprotected { told, rpcs, .. } = &link.notes()[at] else {
            unreachable!("found above");
        };
        assert_eq!(told.as_slice(), [(cid, INST_A)]);
        // No cancel: the registry holds it ended.
        assert!(rpcs.is_empty());
        (cid, at)
    }
}

/// Runs `scenario` against the toy's order entry and the stub serving `venue`, the link under
/// `caps`, on a paused runtime nothing moves but the scenario.
fn play(caps: OrderCaps, venue: Venue, scenario: impl AsyncFnOnce(&mut Run)) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        // A blocked thread keeps the paused clock from moving on its own.
        let (thaw, frozen) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || frozen.recv());
        let clock = IngestClock::new();
        let venue: Shared = Arc::new(Mutex::new(venue));
        let served = Arc::clone(&venue);
        let (script, pusher) = WsScript::serving(move |_conn| {
            let venue = Arc::clone(&served);
            Responder::new(move |frame| respond(&venue, frame))
        });
        let server = StubServer::start(script, HttpRouter::new()).await.unwrap();
        let link = Rc::new(RefCell::new(link_under(caps)));
        let handler = LinkHandler {
            link: Rc::clone(&link),
            wake: Box::new(|| {}),
        };
        let url = server.ws_url("/exec");
        let (mut session, control) = session(&url, None, &clock, handler);
        link.borrow_mut().attach(session.orders(), ACCT);
        let dir = Dir::new();
        let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
        let mut run = Run {
            link,
            orders: session.orders(),
            pusher,
            venue,
            mint: CidMint::new(lease, 0, 0, WallNs(1_000_000_000)),
            dir,
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

/// The toy's answer to query `rpc` showing our order's fields as the venue holds them, in `st`.
fn answer(venue: &Shared, rpc: RpcId, st: &str) -> String {
    let v = lock(venue);
    format!("qres|rpc={}|found=1|{}", rpc.0, v.shown(0, st))
}

/// The query of unprotected `cid` the link noted as it was told: its first ladder note.
fn query_of(link: &Link, cid: ClientOrderId) -> RpcId {
    match link.ladder_notes().first() {
        Some(LadderNote::Query {
            cid: queried,
            rpc,
            of: Queried::Unprotected,
        }) if *queried == cid => *rpc,
        other => panic!("no query of the unprotected order after it was told: {other:?}"),
    }
}

#[test]
fn an_unprotected_order_the_registry_holds_ended_is_queried_and_released_once_shown_ended() {
    play(order_caps(), Venue::default(), async |run| {
        let (cid, at) = run.ended_then_unprotected().await;
        let rpc = query_of(&run.link.borrow(), cid);
        {
            let link = run.link.borrow();
            let Note::Unprotected { left, .. } = &link.notes()[at] else {
                unreachable!("told above");
            };
            assert_eq!(left.len(), 1);
            assert!(
                left[0].contains(&format!("it is queried (request {})", rpc.0)),
                "{left:?}"
            );
            assert!(!left[0].contains("FBC-m8vm"), "{left:?}");
        }
        // Named by the venue id the session showed it under: the toy's queries take it first.
        run.until("the query read", |r| r.read("query").len() == 1)
            .await;
        let query = &run.read("query")[0];
        assert_eq!(get(query, "vid").as_deref(), Some("V-1"), "{query}");
        // Held until the answer comes.
        assert!(!run.orders.may_place_on(INST_A));
        run.push(answer(&run.venue, rpc, "canceled|why=requested"))
            .await;
        run.until("the market released", |r| r.orders.may_place_on(INST_A))
            .await;
        let ended = VenueOrderState::Canceled(CancelReason::Requested);
        assert_eq!(
            run.answers(),
            [(
                cid,
                rpc,
                Queried::Unprotected,
                QueryRead::Shown(ended),
                None
            )]
        );
    });
}

#[test]
fn an_unprotected_order_the_answer_shows_open_keeps_its_market_held() {
    play(order_caps(), Venue::default(), async |run| {
        let (cid, _) = run.ended_then_unprotected().await;
        let rpc = query_of(&run.link.borrow(), cid);
        run.until("the query read", |r| r.read("query").len() == 1)
            .await;
        run.push(answer(&run.venue, rpc, "open")).await;
        run.until("the answer noted", |r| r.answers().len() == 1)
            .await;
        let shown = QueryRead::Shown(VenueOrderState::Open);
        assert_eq!(
            run.answers(),
            [(cid, rpc, Queried::Unprotected, shown, None)]
        );
        // The epoch places, but not on its market.
        assert!(run.orders.may_place());
        assert!(!run.orders.may_place_on(INST_A));
        // The registry still holds it ended: the link builds nothing more for it.
        assert!(run.state(cid).is_terminal());
        assert_eq!(run.read("cancel").len(), 1);
    });
}

#[test]
fn an_unprotected_order_whose_query_is_never_answered_keeps_its_market_held() {
    play(order_caps(), Venue::default(), async |run| {
        let (cid, _) = run.ended_then_unprotected().await;
        let rpc = query_of(&run.link.borrow(), cid);
        run.until("the query read", |r| r.read("query").len() == 1)
            .await;
        // At the toy's request deadline the session reports it Unknown; it is never sent again.
        tokio::time::advance(RPC_TIMEOUT).await;
        run.until("the query's fate noted", |r| r.answers().len() == 1)
            .await;
        let unknown = QueryRead::Outcome(SubmitOutcome::Unknown);
        assert_eq!(
            run.answers(),
            [(cid, rpc, Queried::Unprotected, unknown, None)]
        );
        assert!(!run.orders.may_place_on(INST_A));
        assert_eq!(run.read("query").len(), 1);
    });
}

#[test]
fn an_unprotected_order_with_no_query_reference_in_the_caps_is_noted_and_nothing_is_sent() {
    // The link's caps: the venue's queries name an order by nothing it carries.
    let caps = OrderCaps {
        query_refs: TagSet::none(),
        ..order_caps()
    };
    play(caps, Venue::default(), async |run| {
        let (_, at) = run.ended_then_unprotected().await;
        {
            let link = run.link.borrow();
            let Note::Unprotected { left, .. } = &link.notes()[at] else {
                unreachable!("told above");
            };
            assert_eq!(left.len(), 1);
            let said = "the venue's caps declare no reference for queries that it carries, so no \
                        query is sent";
            assert!(left[0].contains(said), "{left:?}");
            assert_eq!(link.ladder_notes(), []);
        }
        // Moving the clock lets the session run its turns: still nothing is queried, and the
        // market stays held.
        tokio::time::advance(RPC_TIMEOUT).await;
        tokio::task::yield_now().await;
        assert!(run.read("query").is_empty());
        assert!(!run.orders.may_place_on(INST_A));
    });
}

#[test]
fn an_unknown_order_is_queried_through_the_ladder_and_resolved_by_the_answer() {
    let venue = Venue {
        hold_acks: true,
        queries: Answers::FromState,
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let cid = run.place().await;
        // Before its intent timeout nothing is due.
        assert_eq!(run.ladder(), LadderPass::default());
        // Unanswered at the toy's request deadline: the session reports it Unknown, and fbc-oms
        // puts it on the ladder.
        tokio::time::advance(RPC_TIMEOUT).await;
        run.until("the order Unknown", |r| r.state(cid) == OrdState::Unknown)
            .await;
        let pass = run.ladder();
        assert_eq!(pass.queries.len(), 1);
        let (queried, rpc) = pass.queries[0];
        assert_eq!(queried, cid);
        assert!(pass.tombstones.is_empty() && pass.unsent.is_empty() && !pass.resync_wanted);
        let of = Queried::Ladder;
        let noted = LadderNote::Query { cid, rpc, of };
        assert!(run.link.borrow().ladder_notes().contains(&noted));
        // The venue gave it no id the link heard: queried by its placement nonce, which the link
        // recorded from the session's receipt.
        run.until("the query read", |r| r.read("query").len() == 1)
            .await;
        let query = &run.read("query")[0];
        let nonce = lock(&run.venue).orders[0].nonce.clone();
        assert_eq!(get(query, "pnonce"), Some(nonce), "{query}");
        assert_eq!(get(query, "vid"), None, "{query}");
        run.until("the answer noted", |r| r.answers().len() == 1)
            .await;
        let open = QueryRead::Shown(VenueOrderState::Open);
        let resolved = Some(LadderResolution::Resolved(OrdState::Open));
        assert_eq!(run.answers(), [(cid, rpc, Queried::Ladder, open, resolved)]);
        // Off the ladder, resting, by the answer: no Stop, no resend.
        let link = run.link.borrow();
        let record = link.registry().get(cid).unwrap();
        assert_eq!(record.state(), OrdState::Open);
        assert!(record.unknown_since().is_none());
        assert_eq!(
            record.vid().map(|v| v.as_str().to_owned()).as_deref(),
            Some("V-1")
        );
        drop(link);
        assert_eq!(run.ladder(), LadderPass::default());
        assert_eq!(run.read("place").len(), 1);
    });
}

#[test]
fn a_tombstone_due_is_sent_once_by_client_id_and_recorded_so_its_acceptance_ends_the_order() {
    let venue = Venue {
        hold_acks: true,
        queries: Answers::NotFound,
        cancel_answer_only: true,
        ..Venue::default()
    };
    play(order_caps(), venue, async |run| {
        run.started().await;
        let cid = run.place().await;
        tokio::time::advance(RPC_TIMEOUT).await;
        run.until("the order Unknown", |r| r.state(cid) == OrdState::Unknown)
            .await;
        let since = run
            .link
            .borrow()
            .registry()
            .get(cid)
            .unwrap()
            .unknown_since()
            .unwrap();
        let rpc = run.ladder().queries[0].1;
        // The venue does not find it: inconclusive, resyncs decide.
        run.until("the answer noted", |r| r.answers().len() == 1)
            .await;
        let inconclusive = Some(LadderResolution::Inconclusive);
        let answered = (cid, rpc, Queried::Ladder, QueryRead::NotFound, inconclusive);
        assert_eq!(run.answers(), [answered]);
        let pass = run.ladder();
        assert!(pass.resync_wanted && pass.queries.is_empty() && pass.tombstones.is_empty());

        // At the maximum time on the ladder, the tombstone: once.
        let due = since.0 + u64::try_from(MAX_UNKNOWN.as_nanos()).unwrap();
        tokio::time::advance(Duration::from_nanos(due - run.now().0)).await;
        let pass = run.ladder();
        assert_eq!(pass.tombstones.len(), 1);
        let (tombstoned, tombstone) = pass.tombstones[0];
        assert_eq!(tombstoned, cid);
        let tombstones: Vec<(ClientOrderId, RpcId)> = {
            let link = run.link.borrow();
            let noted = link.ladder_notes().iter().filter_map(|n| match n {
                LadderNote::Tombstone { cid, rpc } => Some((*cid, *rpc)),
                _ => None,
            });
            noted.collect()
        };
        assert_eq!(tombstones, [(cid, tombstone)]);
        assert!(run.ladder().tombstones.is_empty());
        // By our client id alone, never an account cancel-all.
        run.until("the tombstone read", |r| r.read("cancel").len() == 1)
            .await;
        let cancel = &run.read("cancel")[0];
        assert_eq!(
            get(cancel, "cid"),
            Some(lock(&run.venue).orders[0].cid.clone()),
            "{cancel}"
        );
        assert_eq!(get(cancel, "vid"), None, "{cancel}");
        assert!(run.read("cancelall").is_empty());
        // Recorded as the ladder's tombstone, so its acceptance for good ends the order with no
        // order event.
        let canceled = OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested));
        run.until("the order ended", |r| r.state(cid) == canceled)
            .await;
        let resolved = LadderNote::TombstoneResolved {
            cid,
            rpc: tombstone,
            state: canceled,
        };
        assert!(run.link.borrow().ladder_notes().contains(&resolved));
        assert!(run.ladder().tombstones.is_empty());
        assert_eq!(run.read("cancel").len(), 1);
    });
}

/// `body`, the `seq`th input, received at `at` on connection 0's first epoch.
fn env(seq: u64, at: u64, body: ExecEvent) -> Envelope<ExecEvent> {
    Envelope {
        stamp: Stamp {
            ingest_seq: seq,
            kernel_rx: None,
            recv_mono: MonoNs(at),
            recv_wall: WallNs(i64::try_from(at).unwrap()),
            conn: ConnKey { conn: 0, epoch: 0 },
        },
        exch_ts: Some(ExchNs(i64::try_from(at).unwrap())),
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: Some(seq),
        body,
    }
}

/// What the link notes as unsent in a pass.
fn unsent(link: &Link) -> Vec<(ClientOrderId, LadderCommand, String)> {
    let unsent = link.ladder_notes().iter().filter_map(|n| match n {
        LadderNote::Unsent { cid, what, why } => Some((*cid, *what, why.clone())),
        _ => None,
    });
    unsent.collect()
}

/// A link and a toy session that is never run, its submissions only queued, with the two orders
/// of ours [`never_run`] placed through it, the mint they came from and the leases' directory.
struct NeverRun {
    link: Link,
    session: (ExecSession<LinkHandler>, ExecControl),
    orders: ExecOrders,
    cids: [ClientOrderId; 2],
    mint: CidMint,
    _dir: Dir,
}

/// A link under `caps` beside a toy session that is never run, resynced flat and Started on
/// `INST_A`; two orders of ours placed through it and reported sent, each receipt naming its
/// placement nonce, then each reported Unknown at its deadline, as the session would, at 2010
/// and 2011 ns on the shard clock.
fn never_run(caps: OrderCaps) -> NeverRun {
    let dir = Dir::new();
    let mut link = link_under(caps);
    let handler = LinkHandler {
        link: Rc::new(RefCell::new(link_under(order_caps()))),
        wake: Box::new(|| {}),
    };
    let clock = IngestClock::new();
    let rest = Some("http://127.0.0.1:9");
    let session = session("ws://127.0.0.1:9/", rest, &clock, handler);
    let orders = session.0.orders();
    for (n, body) in (0u64..).zip([
        ExecEvent::Conn {
            stream: fbc_core::StreamId(0),
            state: fbc_core::ConnState::Authenticated,
        },
        ExecEvent::ResyncBegin {
            watermark: WallNs(1_000),
        },
        ExecEvent::ResyncPosition {
            inst: INST_A,
            qty: fbc_core::SignedLots(0),
            avg_entry: None,
        },
        ExecEvent::ResyncEnd,
    ]) {
        link.on_exec(env(n, 1_000 + n, body));
    }
    start(&mut link, &dir);
    let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let cids = [mint.mint().unwrap(), mint.mint().unwrap()];
    for (n, cid) in (10u64..).zip(cids) {
        let cmd = link.reg().place(new_order(cid)).unwrap();
        let auth = link.reg().authorize(ACCT, cmd).unwrap();
        let rpc = link.submit(&orders, auth).unwrap();
        let ctx = fbc_core::EncodeCtx {
            wall: WallNs(0),
            mono: MonoNs(0),
            nonces: NonceBlock::new(vec![100 + n]),
        };
        let mut receipt = fbc_core::EncodeReceipt::new();
        receipt.use_nonce(&ctx, 0).unwrap();
        link.on_submitted(SubmitHandle {
            rpc,
            receipt: Ok(receipt),
        });
        let item = ItemRef {
            idx: 0,
            cid: Some(cid),
            vid: None,
        };
        let unknown = ExecEvent::Outcome {
            rpc,
            item: Some(item),
            outcome: SubmitOutcome::Unknown,
        };
        link.on_exec(env(n, 2_000 + n, unknown));
    }
    NeverRun {
        link,
        session,
        orders,
        cids,
        mint,
        _dir: dir,
    }
}

#[test]
fn what_a_pass_cannot_send_is_noted_and_a_query_not_sent_leaves_its_order_to_resyncs() {
    let NeverRun {
        mut link,
        session,
        orders,
        cids: [first, second],
        ..
    } = never_run(order_caps());
    let now = MonoNs(3_000);
    let pass = link.ladder(&orders, now);
    let rpcs: Vec<RpcId> = pass.queries.iter().map(|(_, rpc)| *rpc).collect();
    assert_eq!(
        pass.queries.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
        [first, second]
    );
    // The first query reported not sent: the ladder leaves its order to resyncs.
    link.on_submitted(SubmitHandle {
        rpc: rpcs[0],
        receipt: Err(NotSentReason::Disconnected),
    });
    // The second accepted with its answer still to come, then answered with no order found.
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    let accepted_now = ExecEvent::Outcome {
        rpc: rpcs[1],
        item: None,
        outcome: accepted.clone(),
    };
    link.on_exec(env(20, 3_100, accepted_now));
    let target = fbc_core::OrderRef::Client(second);
    let found_none = QueryAnswer::new(rpcs[1], target.clone(), None).unwrap();
    link.on_exec(env(21, 3_200, ExecEvent::QueryResult(found_none)));
    let answers: Vec<(ClientOrderId, QueryRead, Option<LadderResolution>)> = link
        .ladder_notes()
        .iter()
        .filter_map(|n| match n {
            LadderNote::QueryAnswer {
                cid,
                read,
                resolution,
                of: Queried::Ladder,
                ..
            } => Some((*cid, read.clone(), *resolution)),
            _ => None,
        })
        .collect();
    let not_sent = QueryRead::Outcome(SubmitOutcome::NotSent(NotSentReason::Disconnected));
    let inconclusive = Some(LadderResolution::Inconclusive);
    assert_eq!(
        answers,
        [
            (first, not_sent, inconclusive),
            (
                second,
                QueryRead::Outcome(accepted),
                Some(LadderResolution::Ignored)
            ),
            (second, QueryRead::NotFound, inconclusive),
        ]
    );
    // Answered, the queries are forgotten: a later answer to one reaches neither the registry
    // nor a note. Taken, the ladder notes are gone from the link.
    assert_eq!(link.take_ladder_notes().len(), 5);
    let late = QueryAnswer::new(rpcs[1], target, None).unwrap();
    link.on_exec(env(22, 3_300, ExecEvent::QueryResult(late)));
    assert_eq!(link.ladder_notes(), []);

    // The session ended: at the maximum time on the ladder, both tombstones are due and refused
    // by it, each noted with why.
    drop(session);
    let due = MonoNs(2_011 + u64::try_from(MAX_UNKNOWN.as_nanos()).unwrap());
    let pass = link.ladder(&orders, due);
    assert!(pass.resync_wanted && pass.queries.is_empty() && pass.tombstones.is_empty());
    let ended = "Ended".to_owned();
    let refused = [
        (first, LadderCommand::Tombstone, ended.clone()),
        (second, LadderCommand::Tombstone, ended),
    ];
    assert_eq!(pass.unsent, refused);
    assert_eq!(unsent(&link), refused);
}

#[test]
fn a_ladder_query_the_session_refuses_and_a_tombstone_the_venue_cannot_name_are_noted_with_why() {
    // A venue whose single cancel names no client id: no tombstone is built.
    let caps = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::Venue]),
        ..order_caps()
    };
    let NeverRun {
        mut link,
        session,
        orders,
        cids,
        ..
    } = never_run(caps);
    drop(session);
    let pass = link.ladder(&orders, MonoNs(3_000));
    let ended = "Ended".to_owned();
    let refused = cids.map(|cid| (cid, LadderCommand::Query, ended.clone()));
    assert_eq!(pass.unsent, refused);
    assert!(pass.queries.is_empty());
    let due = MonoNs(2_011 + u64::try_from(MAX_UNKNOWN.as_nanos()).unwrap());
    let pass = link.ladder(&orders, due);
    let why = "the venue's single cancel cannot name a client id".to_owned();
    let unnamed = cids.map(|cid| (cid, LadderCommand::Tombstone, why.clone()));
    assert_eq!(pass.unsent, unnamed);
    assert!(pass.tombstones.is_empty());
    let noted: Vec<_> = refused.into_iter().chain(unnamed).collect();
    assert_eq!(unsent(&link), noted);
}

#[test]
fn an_unprotected_order_the_registry_does_not_hold_with_no_session_attached_is_noted_unqueried() {
    let NeverRun {
        mut link, mut mint, ..
    } = never_run(order_caps());
    // An earlier run's order the registry never held, with no session attached to query it.
    let cid = mint.mint().unwrap();
    let vid = dispatch_market_data(&fbc_conformance::toy::caps(), |s| s.venue_order_id("V-9"));
    let snapshot = fbc_core::VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid)),
        vid: vid.unwrap(),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(100)),
        qty: Lots::new(1).unwrap(),
        cum_filled: Lots::ZERO,
        post_only: Some(true),
        reduce_only: Some(false),
    };
    link.on_unprotected(&[snapshot]);
    let Some(Note::Unprotected { told, rpcs, left }) = link.notes().last() else {
        panic!("no unprotected note last: {:?}", link.notes());
    };
    assert_eq!(told.as_slice(), [(cid, INST_A)]);
    assert!(rpcs.is_empty());
    assert_eq!(left.len(), 1);
    assert!(
        left[0].contains("its query was not submitted: no session attached"),
        "{left:?}"
    );
}

#[test]
fn the_safety_cancel_of_an_open_unprotected_order_goes_out_before_the_query_of_an_ended_one() {
    play(order_caps(), Venue::default(), async |run| {
        run.started().await;
        let open = run.place().await;
        let ended = run.place().await;
        run.until("the first acknowledged", |r| {
            r.state(open) == OrdState::Open
        })
        .await;
        run.cancelled(ended).await;
        let at = run.reconnected_stale().await;
        let (cancel, query) = {
            let link = run.link.borrow();
            let Note::Unprotected { told, rpcs, .. } = &link.notes()[at] else {
                unreachable!("told above");
            };
            assert_eq!(told.as_slice(), [(open, INST_A), (ended, INST_A)]);
            assert_eq!(rpcs.len(), 1, "one Safety cancel: {rpcs:?}");
            assert_eq!(link.safety_cancel_of(open), Some(rpcs[0]));
            let query = link.ladder_notes().iter().find_map(|n| match n {
                LadderNote::Query {
                    cid,
                    rpc,
                    of: Queried::Unprotected,
                } if *cid == ended => Some(*rpc),
                _ => None,
            });
            (rpcs[0], query.expect("the ended order queried"))
        };
        // The protective cancel is submitted first, so the session encodes it first (decision
        // 0080: at once), and the venue reads it before the query.
        assert!(cancel.0 < query.0, "cancel {cancel:?}, query {query:?}");
        // The Safety cancel goes as the toy's batch cancel, of one item.
        run.until("both read", |r| {
            r.read("query").len() == 1 && r.read("cancels").len() == 1
        })
        .await;
        let read = lock(&run.venue).read.clone();
        let kind = |l: &String| l.split('|').next().unwrap_or_default().to_owned();
        let cancel_at = read.iter().position(|l| kind(l) == "cancels").unwrap();
        let query_at = read.iter().position(|l| kind(l) == "query").unwrap();
        assert!(cancel_at < query_at, "{read:?}");
        assert_eq!(get(&read[query_at], "vid").as_deref(), Some("V-2"));
        run.until("the open order ended", |r| r.state(open).is_terminal())
            .await;
    });
}

#[test]
fn an_unprotected_order_the_registry_does_not_hold_is_queried_and_released_once_shown_ended() {
    play(order_caps(), Venue::default(), async |run| {
        // An earlier run's order of ours, resting at the venue, which the registry never held.
        // The venue's resync lists it twice, so the registry refuses that resync and holds the
        // order not at all, while the session, whose arm covers no order already open, tells it
        // unprotected.
        let cid = run.mint.mint().unwrap();
        let wire = fbc_core::encode_cid(&order_caps().client_id, cid).unwrap();
        {
            let mut v = lock(&run.venue);
            for _ in 0..2 {
                v.orders.push(Order {
                    cid: wire.as_str().to_owned(),
                    vid: "V-9".to_owned(),
                    side: "B".to_owned(),
                    px: "100".to_owned(),
                    qty: "1".to_owned(),
                    nonce: "0".to_owned(),
                    resting: true,
                });
            }
        }
        let at = run.told().await;
        let rpc = {
            let link = run.link.borrow();
            let refused = |n: &Note| matches!(n, Note::ResyncRefused(_));
            assert!(link.notes().iter().any(refused), "{:?}", link.notes());
            assert!(link.registry().get(cid).is_none());
            let Note::Unprotected { told, rpcs, left } = &link.notes()[at] else {
                unreachable!("told above");
            };
            assert_eq!(told.as_slice(), [(cid, INST_A)]);
            assert!(rpcs.is_empty());
            let rpc = query_of(&link, cid);
            assert_eq!(left.len(), 1);
            assert!(
                left[0].contains(&format!("it is queried (request {})", rpc.0)),
                "{left:?}"
            );
            rpc
        };
        run.until("the query read", |r| r.read("query").len() == 1)
            .await;
        let query = &run.read("query")[0];
        assert_eq!(get(query, "vid").as_deref(), Some("V-9"), "{query}");
        assert!(!run.orders.may_place_on(INST_A));
        run.push_on(0, answer(&run.venue, rpc, "canceled|why=requested"))
            .await;
        run.until("the market released", |r| r.orders.may_place_on(INST_A))
            .await;
        let ended = VenueOrderState::Canceled(CancelReason::Requested);
        assert_eq!(
            run.answers(),
            [(
                cid,
                rpc,
                Queried::Unprotected,
                QueryRead::Shown(ended),
                None
            )]
        );
    });
}

/// The intent timeout [`ladder_config`] sets, in nanoseconds.
fn intent_timeout_ns() -> u64 {
    u64::try_from(ladder_config().intent_timeout().as_nanos()).unwrap()
}

/// The answers to queries `link` noted, by request and resolution.
fn answered(link: &Link) -> Vec<(RpcId, Option<LadderResolution>)> {
    let answers = link.ladder_notes().iter().filter_map(|n| match n {
        LadderNote::QueryAnswer {
            rpc, resolution, ..
        } => Some((*rpc, *resolution)),
        _ => None,
    });
    answers.collect()
}

/// The acceptance of request `rpc` for good, with its answer still to come.
fn accepted(rpc: RpcId) -> ExecEvent {
    ExecEvent::Outcome {
        rpc,
        item: None,
        outcome: SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        },
    }
}

/// An answer to query `rpc` finding no order.
fn found_none(rpc: RpcId, cid: ClientOrderId) -> ExecEvent {
    let target = fbc_core::OrderRef::Client(cid);
    ExecEvent::QueryResult(QueryAnswer::new(rpc, target, None).unwrap())
}

#[test]
fn an_acknowledged_ladder_query_never_answered_is_dropped_once_the_ladder_gives_it_up() {
    let NeverRun {
        mut link,
        session: _session,
        orders,
        cids: [first, second],
        ..
    } = never_run(order_caps());
    let pass = link.ladder(&orders, MonoNs(3_000));
    let rpcs: Vec<RpcId> = pass.queries.iter().map(|(_, rpc)| *rpc).collect();
    assert_eq!(rpcs.len(), 2);
    // Both acknowledged, which clears their deadlines: their answers may never come.
    link.on_exec(env(20, 3_100, accepted(rpcs[0])));
    link.on_exec(env(21, 3_101, accepted(rpcs[1])));
    link.take_ladder_notes();
    // Before the intent timeout fbc-oms still awaits both: a pass keeps them, and an answer
    // reaches the registry.
    let pass = link.ladder(&orders, MonoNs(3_000 + intent_timeout_ns() - 1));
    assert!(pass.queries.is_empty() && !pass.resync_wanted);
    link.on_exec(env(22, 3_200, found_none(rpcs[1], second)));
    let inconclusive = Some(LadderResolution::Inconclusive);
    assert_eq!(answered(&link), [(rpcs[1], inconclusive)]);
    link.take_ladder_notes();
    // At the intent timeout fbc-oms gives the first up for resyncs; the link forgets it, so a
    // late answer reaches neither the registry nor a note.
    let pass = link.ladder(&orders, MonoNs(3_000 + intent_timeout_ns()));
    assert!(pass.resync_wanted && pass.queries.is_empty());
    link.on_exec(env(23, 3_300, found_none(rpcs[0], first)));
    assert_eq!(link.ladder_notes(), []);
}

/// An earlier run's order of ours, `cid`, resting under venue id `vid` on `INST_A`, as a
/// resync shows it.
fn earlier_runs(cid: ClientOrderId, vid: &str) -> fbc_core::VenueOrderSnapshot {
    let vid = dispatch_market_data(&fbc_conformance::toy::caps(), |s| s.venue_order_id(vid));
    fbc_core::VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid)),
        vid: vid.unwrap(),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(100)),
        qty: Lots::new(1).unwrap(),
        cum_filled: Lots::ZERO,
        post_only: Some(true),
        reduce_only: Some(false),
    }
}

#[test]
fn an_acknowledged_unprotected_query_never_answered_is_dropped_after_the_intent_timeout() {
    let dir = Dir::new();
    let mut link = link_under(order_caps());
    let handler = LinkHandler {
        link: Rc::new(RefCell::new(link_under(order_caps()))),
        wake: Box::new(|| {}),
    };
    let clock = IngestClock::new();
    let (session, _control) = session("ws://127.0.0.1:9/", None, &clock, handler);
    let orders = session.orders();
    link.attach(session.orders(), ACCT);
    let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let cids = [mint.mint().unwrap(), mint.mint().unwrap()];
    let before = link.now().unwrap().0;
    link.on_unprotected(&[earlier_runs(cids[0], "V-8"), earlier_runs(cids[1], "V-9")]);
    let after = link.now().unwrap().0;
    let rpcs: Vec<RpcId> = link
        .ladder_notes()
        .iter()
        .filter_map(|n| match n {
            LadderNote::Query {
                rpc,
                of: Queried::Unprotected,
                ..
            } => Some(*rpc),
            _ => None,
        })
        .collect();
    assert_eq!(rpcs.len(), 2);
    link.on_exec(env(1, 10, accepted(rpcs[0])));
    link.on_exec(env(2, 11, accepted(rpcs[1])));
    link.take_ladder_notes();
    // Before the intent timeout both are kept: an answer is still noted.
    link.ladder(&orders, MonoNs(before.0 + intent_timeout_ns() - 1));
    link.on_exec(env(3, 12, found_none(rpcs[1], cids[1])));
    assert_eq!(answered(&link), [(rpcs[1], None)]);
    link.take_ladder_notes();
    // At it, the acknowledged one left unanswered is forgotten: a late answer is not noted.
    link.ladder(&orders, MonoNs(after.0 + intent_timeout_ns()));
    link.on_exec(env(4, 13, found_none(rpcs[0], cids[0])));
    assert_eq!(link.ladder_notes(), []);
}

#[test]
fn no_text_in_the_glue_says_its_order_query_is_unbuilt() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut read = 0;
    for file in std::fs::read_dir(src).unwrap() {
        let path = file.unwrap().path();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            !text.contains("FBC-m8vm"),
            "{} names FBC-m8vm",
            path.display()
        );
        read += 1;
    }
    assert!(read > 0);
}
