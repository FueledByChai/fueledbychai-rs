//! The link's retention pass (FBC-6pdo, decision 0157): `Link::retain_ended` runs fbc-oms's
//! `Registry::retain_ended` under the link's order caps, and what the link reads and notes of an
//! order around and after its forget.
//!
//! - An ended order behind on its fills stays held, so the stream check still sees it behind.
//! - A cancel-many's orders stay held while its authorization lives; once the session reports it
//!   sent, the cancel and its answer note no problem, and the orders are then forgotten.
//! - After the forget, a request the link still holds names the order, and a straggler answer
//!   to it, after a provisional acceptance or after `Unknown`, is noted as ours with no problem,
//!   for an order the registry built and for one a resync registered; once the link drops the
//!   request, a straggler is the session's own.
//! - A late event naming a forgotten order is noted as one of an order never held.
//!
//! Every command goes through an fbc-oms authorization and a real `ExecSession`'s `ExecOrders`
//! on the conformance toy, each session never run: the test hands the link what the session
//! would report, then drops the session, which drops the command it still holds, as a session
//! does once it has reported it. The numbers are test inputs on the toy's synthetic market.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_conformance::toy::{EXEC_URL_KEY, INST_A, OWN_NS, REST_URL_KEY, SYMBOL_A, ToyFactory};
use fbc_core::{
    AccountKey, AccountLease, AckLevel, AssetSym, CancelReason, Channel, CidMatch, CidMint,
    ClientOrderId, ConnKey, ConnState, EncodeReceipt, Envelope, ExchNs, ExchTsKind, ExecEvent,
    FillEvent, FillIdent, FillOrigin, ItemRef, Liquidity3, Lots, MarketLease, MonoNs,
    NamespaceLease, NewOrder, NonceBlock, NonceSource, OrderCaps, OrderKind, OrderUpdate, Reject,
    RejectKind, RpcId, Secrets, Side, SignedLots, Stamp, StreamId, SubmitHandle, SubmitOutcome,
    Ticks, Tif, VenueCaps, VenueConfig, VenueFactory, VenueOrderId, VenueOrderSnapshot,
    VenueOrderState, WallNs, dispatch_market_data,
};
use fbc_live::link::{Link, LinkHandler, Note};
use fbc_live::reconcile::{Unreconciled, unreconciled};
use fbc_oms::{
    Applied, Authorization, CancelChoice, FillLedger, LadderConfig, LeaseKeys, Leases,
    LedgerConfig, MarketCapsConfig, PreTradeCaps, Registry, Retained, RetentionConfig, Routed,
};
use fbc_runtime::{
    Connector, ExecControl, ExecSession, ExecSessionConfig, InFlight, IngestClock, ProxyConfig,
    RateLimiter, ReconnectPacing, ReservedBound, ResyncDeadline, RpcIds, SafetyReserve, WriteStall,
};

static TOY: ToyFactory = ToyFactory;

/// The account the sessions trade and the registry authorizes for.
const ACCT: AccountKey = AccountKey::new(1);

const KEY: ConnKey = ConnKey { conn: 0, epoch: 0 };

/// The consumer's resync bound in these tests; the toy's snapshot lag bound is zero.
const RESYNC: Duration = Duration::from_millis(300);

/// When the first pass of each test runs, on the shard's monotonic clock.
const T1: MonoNs = MonoNs(10_000_000_000);

/// The toy's capabilities.
fn toy() -> VenueCaps {
    fbc_conformance::toy::caps()
}

/// The toy's order caps.
fn order_caps() -> OrderCaps {
    toy().exec.unwrap().order
}

/// The consumer's retention bounds: a count no test reaches, an age of a millisecond and
/// [`RESYNC`], so a record no pin holds is forgotten once its floor has passed.
fn retention() -> RetentionConfig {
    RetentionConfig::new(1_000, Duration::from_millis(1), RESYNC).unwrap()
}

/// A directory removed when it is dropped.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Dir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "fbc-live-retention-test-{}-{n}",
            std::process::id()
        ));
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

/// The link's ledger: an hour's fills, a thousand at most.
fn ledger() -> FillLedger {
    let cfg = LedgerConfig {
        max_age: Duration::from_secs(3600),
        max_entries: 1_000,
    };
    FillLedger::new(cfg, WallNs(0)).unwrap()
}

/// The link's Unknown ladder.
fn ladder() -> LadderConfig {
    LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap()
}

/// An order-entry session on the toy for `ACCT` on `clock`, its request ids from `rpc_ids`,
/// never run: its URLs are loopback ports nothing listens on, and its handler holds a link of
/// its own, which nothing reaches.
fn session(clock: &IngestClock, rpc_ids: &RpcIds) -> (ExecSession<LinkHandler>, ExecControl) {
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, "ws://127.0.0.1:9/");
    cfg.insert(REST_URL_KEY, "http://127.0.0.1:9");
    let limits = TOY.caps(&cfg).unwrap().limits;
    let ms = Duration::from_millis;
    let handler = LinkHandler {
        link: Rc::new(RefCell::new(Link::new(
            Registry::new(),
            ledger(),
            order_caps(),
            ladder(),
        ))),
        wake: Box::new(|| {}),
    };
    let config = ExecSessionConfig {
        venue: &TOY,
        cfg,
        creds: Secrets::new(),
        acct: ACCT,
        account_wide_cod_authority: None,
        rpc_ids: rpc_ids.clone(),
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
        http_max_body: 4096,
        outbound: ReservedBound::new(64, 64).unwrap(),
        in_flight: InFlight::new(ReservedBound::new(64, 64).unwrap()),
    };
    ExecSession::new(config, handler).unwrap()
}

/// `body`, the `seq`th input, received at `at` on connection 0's first epoch.
fn env(seq: u64, at: u64, body: ExecEvent) -> Envelope<ExecEvent> {
    Envelope {
        stamp: Stamp {
            ingest_seq: seq,
            kernel_rx: None,
            recv_mono: MonoNs(at),
            recv_wall: WallNs(i64::try_from(at).unwrap()),
            conn: KEY,
        },
        exch_ts: Some(ExchNs(i64::try_from(at).unwrap())),
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: Some(seq),
        body,
    }
}

fn conn(state: ConnState) -> ExecEvent {
    ExecEvent::Conn {
        stream: StreamId(0),
        state,
    }
}

/// A venue order id, built as a codec builds one: through the decode scope.
fn vid(text: &str) -> VenueOrderId {
    dispatch_market_data(&toy(), |scope| scope.venue_order_id(text)).unwrap()
}

/// Sent, with no nonce named.
fn sent(rpc: RpcId) -> SubmitHandle {
    SubmitHandle {
        rpc,
        receipt: Ok(EncodeReceipt::new()),
    }
}

/// The problems the link noted, in order.
fn problems(link: &Link) -> Vec<String> {
    link.notes()
        .iter()
        .filter_map(|n| match n {
            Note::Problem(what) => Some(what.clone()),
            _ => None,
        })
        .collect()
}

/// A registry for `ACCT` on the toy, `INST_A` its one market under caps of 10 lots, its
/// market and account lease names "toy".
fn registry() -> Registry {
    let caps = order_caps();
    let symbol = dispatch_market_data(&toy(), |s| s.venue_symbol(SYMBOL_A)).unwrap();
    let keys = LeaseKeys::new("toy", "test-acct", &caps).with_market(INST_A, symbol);
    let config = MarketCapsConfig {
        inventory: Lots::new(10),
        resting: Lots::new(10),
    };
    let market = PreTradeCaps::new().with_market(INST_A, config).unwrap();
    Registry::with_caps(market).with_lease_keys(keys)
}

/// Our order `cid` on `INST_A` as venue order `v` (or naming none), a buy of a lot at 100, in
/// `state` with the venue's cumulative fill `cum`.
fn update(
    cid: Option<ClientOrderId>,
    v: Option<&str>,
    state: VenueOrderState,
    cum: i64,
) -> ExecEvent {
    ExecEvent::Order(OrderUpdate {
        cid: cid.map(CidMatch::Ours),
        vid: v.map(vid),
        inst: INST_A,
        side: Side::Buy,
        state,
        cum_filled: Lots::new(cum).unwrap(),
        px: Some(Ticks(100)),
        qty: Some(Lots::new(1).unwrap()),
        post_only: Some(true),
        reduce_only: Some(false),
    })
}

/// Canceled by the venue itself, which no cancel of ours is waited on for.
fn by_venue() -> VenueOrderState {
    VenueOrderState::Canceled(CancelReason::Venue)
}

/// A buy fill of a lot of our order `cid` on `INST_A`, keyed by the venue's fill id `id`.
fn fill(cid: ClientOrderId, id: &str) -> FillEvent {
    let caps = toy();
    FillEvent {
        ident: FillIdent::Venue {
            fill: dispatch_market_data(&caps, |s| s.fill_id(id)).unwrap(),
            vid: None,
            cum_after: None,
        },
        origin: FillOrigin::Order {
            cid: Some(CidMatch::Ours(cid)),
            px: Ticks(100),
        },
        inst: INST_A,
        side: Side::Buy,
        qty: Lots::new(1).unwrap(),
        liquidity: Liquidity3::Maker,
        fee: dispatch_market_data(&caps, |s| s.fee(0, AssetSym::new("USDC").unwrap())).unwrap(),
        realized_pnl: None,
        realized_funding: None,
        replay: false,
    }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

fn provisional() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Provisional,
    }
}

/// The link over `registry()`, beside the sessions on the toy it submits through, all on one
/// shard clock and one account's request ids, a market lease and an account lease on `INST_A`
/// (held by the registry once started), and a mint under the toy's own namespace.
struct Rig {
    link: Link,
    clock: IngestClock,
    rpc_ids: RpcIds,
    /// The sessions submitted through and not yet ended.
    sessions: Vec<(ExecSession<LinkHandler>, ExecControl)>,
    mint: CidMint,
    /// The next ingest sequence and receive time handed to the link.
    seq: u64,
    _dir: Dir,
}

impl Rig {
    /// The link over `registry()`, authenticated, resynced flat (the market's seed) and
    /// Started on `INST_A`.
    fn started() -> Rig {
        let dir = Dir::new();
        let link = Link::new(registry(), ledger(), order_caps(), ladder());
        let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
        let mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
        let mut rig = Rig {
            link,
            clock: IngestClock::new(),
            rpc_ids: RpcIds::default(),
            sessions: Vec::new(),
            mint,
            seq: 0,
            _dir: dir,
        };
        rig.resync(&[]);
        let symbol = dispatch_market_data(&toy(), |s| s.venue_symbol(SYMBOL_A)).unwrap();
        let market = MarketLease::acquire(&rig._dir.0, "toy", "test-acct", &symbol).unwrap();
        let account = AccountLease::acquire(&rig._dir.0, "toy", "test-acct").unwrap();
        rig.link
            .reg()
            .start(INST_A, Leases::market(market).with_account(account))
            .unwrap();
        rig
    }

    /// Hands `body` to the link as the next input.
    fn feed(&mut self, body: ExecEvent) {
        self.seq += 1;
        let at = 1_000 + self.seq;
        self.link.on_exec(env(self.seq, at, body));
    }

    /// The socket authenticated again, then a resync showing `orders` open and the position
    /// flat.
    fn resync(&mut self, orders: &[VenueOrderSnapshot]) {
        self.feed(conn(ConnState::Open));
        self.feed(conn(ConnState::Authenticated));
        let watermark = WallNs(i64::try_from(1_000 + self.seq).unwrap());
        self.feed(ExecEvent::ResyncBegin { watermark });
        for o in orders {
            self.feed(ExecEvent::ResyncOrder(o.clone()));
        }
        self.feed(ExecEvent::ResyncPosition {
            inst: INST_A,
            qty: SignedLots(0),
            avg_entry: None,
        });
        self.feed(ExecEvent::ResyncEnd);
    }

    /// Submits `auth` through a fresh session on the rig's clock, which holds it until the
    /// session ends ([`Rig::end_sessions`]): its request id.
    fn submit(&mut self, auth: Authorization) -> RpcId {
        let session = session(&self.clock, &self.rpc_ids);
        let rpc = self.link.submit(&session.0.orders(), auth).unwrap();
        self.sessions.push(session);
        rpc
    }

    /// Ends every session submitted through: each drops the commands it still holds, as a
    /// session does once it has reported them.
    fn end_sessions(&mut self) {
        self.sessions.clear();
    }

    /// Our request `rpc`'s item 0, naming `cid` (and `v`), answered `outcome`.
    fn answer(&mut self, rpc: RpcId, cid: ClientOrderId, v: Option<&str>, outcome: SubmitOutcome) {
        let item = ItemRef {
            idx: 0,
            cid: Some(cid),
            vid: v.map(vid),
        };
        self.feed(ExecEvent::Outcome {
            rpc,
            item: Some(item),
            outcome,
        });
    }

    /// One post-only buy of a lot at 100 on `INST_A`, authorized, submitted, reported sent,
    /// accepted as venue order `v` and shown open: its client id.
    fn open(&mut self, v: &str) -> ClientOrderId {
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
        let cmd = self.link.reg().place(new).unwrap();
        let auth = self.link.reg().authorize(ACCT, cmd).unwrap();
        let rpc = self.submit(auth);
        self.link.on_submitted(sent(rpc));
        self.answer(rpc, cid, Some(v), accepted());
        self.feed(update(Some(cid), Some(v), VenueOrderState::Open, 0));
        cid
    }

    /// The cancel of our order `cid`, authorized and submitted, not yet reported sent.
    fn cancel(&mut self, cid: ClientOrderId) -> RpcId {
        let caps = order_caps();
        let cancel = match self.link.reg().cancellable(cid).unwrap().cancel(&caps) {
            CancelChoice::Send(cmd) => cmd,
            CancelChoice::AwaitAck => panic!("the toy cancels by client id"),
        };
        let auth = self.link.reg().authorize(ACCT, cancel).unwrap();
        self.submit(auth)
    }

    /// A retention pass at `now`.
    fn pass(&mut self, now: MonoNs) -> Retained {
        self.link.retain_ended(&retention(), now)
    }

    /// A pass at [`T1`], which stamps every ended record, then one at its floor (the toy's zero
    /// lag bound plus [`RESYNC`]): what the second did.
    fn passes(&mut self) -> Retained {
        let first = self.pass(T1);
        assert!(first.forgotten.is_empty(), "{first:?}");
        self.pass(T1 + RESYNC)
    }
}

/// `cids`, sorted, as a pass lists what it forgot.
fn sorted(mut cids: Vec<ClientOrderId>) -> Vec<ClientOrderId> {
    cids.sort_unstable();
    cids
}

#[test]
fn an_ended_order_behind_on_its_fills_is_kept_for_the_stream_check() {
    let mut rig = Rig::started();
    // Filled by its order event before its fill arrives: the venue's count is ahead.
    let behind = rig.open("v-behind");
    rig.feed(update(
        Some(behind),
        Some("v-behind"),
        VenueOrderState::Filled,
        1,
    ));
    // The control, cancelled by the venue and owing nothing.
    let control = rig.open("v-control");
    rig.feed(update(Some(control), Some("v-control"), by_venue(), 0));
    rig.end_sessions();

    let retained = rig.passes();
    assert_eq!(retained.forgotten, vec![control], "{retained:?}");
    assert_eq!(retained.pinned.fills_owed, 1, "{retained:?}");
    assert_eq!(retained.settling, 0, "{retained:?}");
    let reg = rig.link.registry();
    assert!(reg.get(behind).is_some() && reg.get(control).is_none());
    let inventory = reg.inventory(INST_A);
    let mut ended = vec![behind];
    assert_eq!(
        unreconciled(reg, None, inventory, &[], &mut ended),
        Some(Unreconciled::Fills {
            cid: behind,
            venue: Lots::new(1).unwrap(),
            read: Lots::ZERO,
        })
    );
    assert_eq!(ended, vec![behind]);

    // Its fill counts it, the stream check agrees, and the next pass forgets it.
    rig.feed(ExecEvent::Fill(fill(behind, "f-behind")));
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Fill { ours: true, .. })
    ));
    assert_eq!(
        rig.pass(T1 + RESYNC + Duration::from_millis(1)).forgotten,
        vec![behind]
    );
    let reg = rig.link.registry();
    assert_eq!(
        unreconciled(reg, None, reg.inventory(INST_A), &[], &mut ended),
        None
    );
    assert!(ended.is_empty());
    assert_eq!(reg.inventory(INST_A), SignedLots(1));
    assert!(problems(&rig.link).is_empty(), "{:?}", problems(&rig.link));
}

#[test]
fn a_cancel_manys_orders_are_kept_while_its_authorization_lives_and_its_answer_is_no_problem() {
    let mut rig = Rig::started();
    let a = rig.open("v-a");
    let b = rig.open("v-b");
    rig.end_sessions();
    let caps = order_caps();
    let mut plan = rig.link.reg().cancel_many(&[a, b], &caps);
    assert_eq!(plan.commands.len(), 1, "one cancel-many: {plan:?}");
    let cmd = plan.commands.remove(0);
    let auth = rig.link.reg().authorize(ACCT, cmd).unwrap();
    // Both end before the cancel-many goes out, the venue's own cancels.
    rig.feed(update(Some(a), Some("v-a"), by_venue(), 0));
    rig.feed(update(Some(b), Some("v-b"), by_venue(), 0));

    // Its authorization names both: every pass keeps them, under `named`.
    let retained = rig.passes();
    assert!(retained.forgotten.is_empty(), "{retained:?}");
    assert_eq!(retained.pinned.named, 2, "{retained:?}");
    assert!(rig.link.registry().get(a).is_some() && rig.link.registry().get(b).is_some());

    // Submitted and reported sent: the cancel-many is recorded on both, which owe its answer.
    let rpc = rig.submit(auth);
    rig.link.on_submitted(sent(rpc));
    rig.end_sessions();
    let later = T1 + RESYNC + Duration::from_millis(1);
    let retained = rig.pass(later);
    assert!(retained.forgotten.is_empty(), "{retained:?}");
    assert_eq!(retained.pinned.answer_owed, 2, "{retained:?}");
    assert_eq!(sorted(rig.link.cids_of(rpc)), sorted(vec![a, b]));

    // Its answer, for the whole request, frees both, and the next pass forgets them.
    rig.feed(ExecEvent::Outcome {
        rpc,
        item: None,
        outcome: accepted(),
    });
    let retained = rig.pass(later + Duration::from_millis(1));
    assert_eq!(retained.forgotten, sorted(vec![a, b]), "{retained:?}");
    assert!(problems(&rig.link).is_empty(), "{:?}", problems(&rig.link));
}

/// The cancel of our order `cid`, submitted, reported sent and answered `first`, then `cid`
/// ended by that cancel: the cancel's request id.
fn cancelled(rig: &mut Rig, cid: ClientOrderId, v: &str, first: SubmitOutcome) -> RpcId {
    let rpc = rig.cancel(cid);
    rig.link.on_submitted(sent(rpc));
    rig.answer(rpc, cid, Some(v), first);
    let requested = VenueOrderState::Canceled(CancelReason::Requested);
    rig.feed(update(Some(cid), Some(v), requested, 0));
    assert!(
        rig.link
            .registry()
            .get(cid)
            .is_some_and(|r| r.state().is_terminal())
    );
    rpc
}

/// Our order shown open as venue order `v` by a later resync, an orphan the registry registers
/// from its snapshot: its client id.
fn registered(rig: &mut Rig, v: &str) -> ClientOrderId {
    let cid = rig.mint.mint().unwrap();
    let row = VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid)),
        vid: vid(v),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(100)),
        qty: Lots::new(1).unwrap(),
        cum_filled: Lots::ZERO,
        post_only: None,
        reduce_only: None,
    };
    rig.resync(&[row]);
    assert!(rig.link.registry().get(cid).is_some(), "registered");
    cid
}

/// After the forget, a straggler answer to a request the link still holds, for each of two
/// orders built here or registered from a resync's snapshot: one whose cancel was accepted
/// provisionally, then finally; one whose cancel went `Unknown`, then was answered.
fn stragglers_after_the_forget(from_snapshot: bool) {
    let mut rig = Rig::started();
    let [x, y] = ["v-x", "v-y"].map(|v| {
        if from_snapshot {
            registered(&mut rig, v)
        } else {
            rig.open(v)
        }
    });
    let to_x = cancelled(&mut rig, x, "v-x", provisional());
    let to_y = cancelled(&mut rig, y, "v-y", SubmitOutcome::Unknown);
    rig.end_sessions();
    // The consumer's forget takes the notes, the answers' included: each request is held until
    // the next forget.
    let len = rig.link.notes().len();
    rig.link.forget(len);
    let retained = rig.passes();
    assert_eq!(retained.forgotten, sorted(vec![x, y]), "{retained:?}");
    assert_eq!(rig.link.cids_of(to_x), vec![x]);
    assert_eq!(rig.link.cids_of(to_y), vec![y]);

    // The stragglers: each noted as ours, with no problem.
    rig.answer(to_x, x, Some("v-x"), accepted());
    rig.answer(to_y, y, Some("v-y"), accepted());
    for rpc in [to_x, to_y] {
        let ours = |n: &Note| matches!(n, Note::Outcome { rpc: r, ours: true, .. } if *r == rpc);
        assert!(rig.link.notes().iter().any(ours), "{rpc:?}");
    }
    assert!(problems(&rig.link).is_empty(), "{:?}", problems(&rig.link));

    // Two forgets drop both requests: a straggler then is the session's own.
    for _ in 0..2 {
        let len = rig.link.notes().len();
        rig.link.forget(len);
    }
    assert!(rig.link.cids_of(to_x).is_empty());
    let reject = Reject {
        kind: RejectKind::Other,
        venue_code: None,
        raw: "late".into(),
    };
    rig.answer(to_x, x, Some("v-x"), SubmitOutcome::Rejected(reject));
    assert!(matches!(
        rig.link.notes(),
        [Note::Outcome { rpc, ours: false, .. }] if *rpc == to_x
    ));
}

#[test]
fn a_straggler_answer_naming_an_order_built_here_and_forgotten_is_ours_and_no_problem() {
    stragglers_after_the_forget(false);
}

#[test]
fn a_straggler_answer_naming_an_order_a_resync_registered_and_forgotten_is_ours_and_no_problem() {
    stragglers_after_the_forget(true);
}

#[test]
fn late_events_naming_a_forgotten_order_are_noted_as_of_an_order_never_held() {
    let mut rig = Rig::started();
    // `y` ends; `x`'s own events name `y`'s venue id under `x`'s client id, which the index
    // keeps giving to `y`; `z` ends with nothing shared.
    let y = rig.open("v-y");
    rig.feed(update(Some(y), Some("v-y"), by_venue(), 0));
    let x = rig.open("v-x");
    rig.feed(update(Some(x), Some("v-y"), VenueOrderState::Open, 0));
    rig.feed(update(Some(x), Some("v-y"), by_venue(), 0));
    let z = rig.open("v-z");
    rig.feed(update(Some(z), Some("v-z"), by_venue(), 0));
    rig.end_sessions();
    let retained = rig.passes();
    assert_eq!(retained.forgotten, sorted(vec![x, z]), "{retained:?}");
    assert_eq!(retained.pinned.shared_vid, 1, "y: {retained:?}");
    let before = rig.link.registry().inventory(INST_A);

    // A straggler terminal update under its client id: an orphan's.
    rig.feed(update(Some(z), Some("v-z"), by_venue(), 0));
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Orphan { cid, vid: Some(v) }) if *cid == z && *v == vid("v-z")
    ));
    // One under its client id naming a venue id the index gives to another ended order held:
    // that order's, late.
    rig.feed(update(Some(x), Some("v-y"), by_venue(), 0));
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Order(Routed::Ours(cid, Applied::IgnoredLate))) if *cid == y
    ));
    // A terminal update naming its venue id alone: untracked, nothing in view.
    rig.feed(update(None, Some("v-z"), by_venue(), 0));
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Order(Routed::Untracked))
    ));
    assert!(!rig.link.registry().foreign_in_view(INST_A));
    // A late fill under its client id: unexplained, and the inventory moves once.
    rig.feed(ExecEvent::Fill(fill(z, "f-z")));
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Fill {
            unexplained: true,
            ours: false,
            ..
        })
    ));
    let reg = rig.link.registry();
    assert_eq!(reg.inventory(INST_A), SignedLots(before.0 + 1));
    assert!(reg.position_known(INST_A));
    assert!(problems(&rig.link).is_empty(), "{:?}", problems(&rig.link));
}
