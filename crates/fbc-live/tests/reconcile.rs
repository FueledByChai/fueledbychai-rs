//! The stream check (`fbc_live::reconcile`, FBC-1awz; the owner's condition in decision 0094,
//! decision 0155 items 2 and 3): a position the venue's stream reported other than the
//! inventory, none streamed yet agreeing; an order of ours whose cumulative fill the venue
//! reported on an order event is above the fills read, until the fill stream brings them; the
//! orders the consumer watches checked before those that ended behind, each of those dropped
//! once it is no longer behind or the registry no longer holds it, their order kept. Every
//! answer comes from what the test passes, the same each time it is asked: the check takes no
//! time and reads no clock (`tests/clock_lint.rs` holds its source to naming none). The orders
//! are the conformance toy's, each through an fbc-oms authorization and a real `ExecSession`'s
//! `ExecOrders`, the session never run: the test hands the link what the session would report.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_conformance::toy::{EXEC_URL_KEY, INST_A, OWN_NS, REST_URL_KEY, SYMBOL_A, ToyFactory};
use fbc_core::{
    AccountKey, AccountLease, AckLevel, AssetSym, Channel, CidMatch, CidMint, ClientOrderId,
    ConnKey, ConnState, EncodeReceipt, Envelope, ExchNs, ExchTsKind, ExecEvent, FillEvent,
    FillIdent, FillOrigin, ItemRef, Liquidity3, Lots, MarketLease, MonoNs, NamespaceLease,
    NewOrder, NonceBlock, NonceSource, OrderKind, OrderUpdate, RpcId, Secrets, Side, SignedLots,
    Stamp, StreamId, SubmitHandle, SubmitOutcome, Ticks, Tif, VenueConfig, VenueFactory,
    VenueOrderId, VenueOrderState, WallNs, dispatch_market_data,
};
use fbc_live::link::{Link, LinkHandler};
use fbc_live::reconcile::{Unreconciled, fills_behind, unreconciled};
use fbc_oms::{
    FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig, MarketCapsConfig, PreTradeCaps,
    Registry,
};
use fbc_runtime::{
    Connector, ExecControl, ExecOrders, ExecSession, ExecSessionConfig, InFlight, IngestClock,
    ProxyConfig, RateLimiter, ReconnectPacing, ReservedBound, ResyncDeadline, RpcIds,
    SafetyReserve, WriteStall,
};

static TOY: ToyFactory = ToyFactory;

/// The account the session trades and the registry authorizes for.
const ACCT: AccountKey = AccountKey::new(1);

const KEY: ConnKey = ConnKey { conn: 0, epoch: 0 };

/// A directory removed when it is dropped.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Dir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "fbc-live-reconcile-test-{}-{n}",
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

/// The ledger's and the ladder's configurations, the consumer's.
fn ledger_and_ladder() -> (FillLedger, LadderConfig) {
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
    (ledger, ladder)
}

/// A link over an empty registry, for the session's own handler, which is never run.
fn idle_link() -> Link {
    let (ledger, ladder) = ledger_and_ladder();
    let caps = fbc_conformance::toy::caps().exec.unwrap().order;
    Link::new(Registry::new(), ledger, caps, ladder)
}

/// An order-entry session on the toy for `ACCT`, never run: its URLs are loopback ports nothing
/// listens on.
fn session() -> (ExecSession<LinkHandler>, ExecControl) {
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, "ws://127.0.0.1:9/");
    cfg.insert(REST_URL_KEY, "http://127.0.0.1:9");
    let limits = TOY.caps(&cfg).unwrap().limits;
    let ms = Duration::from_millis;
    let handler = LinkHandler {
        link: Rc::new(RefCell::new(idle_link())),
        wake: Box::new(|| {}),
    };
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
        clock: IngestClock::new(),
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

/// A registry for `ACCT` on the toy, `INST_A` its one market under caps of 20 lots, room for four
/// orders of 3 lots.
fn registry() -> Registry {
    let caps = fbc_conformance::toy::caps().exec.unwrap().order;
    let symbol =
        dispatch_market_data(&fbc_conformance::toy::caps(), |s| s.venue_symbol(SYMBOL_A)).unwrap();
    let keys = LeaseKeys::new("toy", "test-acct", &caps).with_market(INST_A, symbol);
    let config = MarketCapsConfig {
        inventory: Lots::new(20),
        resting: Lots::new(20),
    };
    let market = PreTradeCaps::new().with_market(INST_A, config).unwrap();
    Registry::with_caps(market).with_lease_keys(keys)
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

/// A venue order id, built as a codec builds one: through the decode scope.
fn vid(text: &str) -> VenueOrderId {
    dispatch_market_data(&fbc_conformance::toy::caps(), |scope| {
        scope.venue_order_id(text)
    })
    .unwrap()
}

/// Our buy `cid` of 3 lots, venue order `v`, in `state` with `cum` lots filled as its order
/// event reports.
fn update(cid: ClientOrderId, v: &str, state: VenueOrderState, cum: i64) -> ExecEvent {
    ExecEvent::Order(OrderUpdate {
        cid: Some(CidMatch::Ours(cid)),
        vid: Some(vid(v)),
        inst: INST_A,
        side: Side::Buy,
        state,
        cum_filled: Lots::new(cum).unwrap(),
        px: Some(Ticks(100)),
        qty: Some(Lots::new(3).unwrap()),
        post_only: Some(true),
        reduce_only: Some(false),
    })
}

/// A buy fill of one lot of our order `cid`, keyed by the venue's fill id `id`.
fn fill(cid: ClientOrderId, id: &str) -> ExecEvent {
    let caps = fbc_conformance::toy::caps();
    ExecEvent::Fill(FillEvent {
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
    })
}

/// A link over `registry()`, authenticated, resynced flat and Started on `INST_A`, beside a
/// session on the toy whose `ExecOrders` it submits through, and a mint under the toy's own
/// namespace. Each input it hands the link takes the next ingest sequence.
struct Rig {
    link: Link,
    orders: ExecOrders,
    mint: CidMint,
    seq: u64,
    _session: (ExecSession<LinkHandler>, ExecControl),
    _dir: Dir,
}

impl Rig {
    fn started() -> Rig {
        let dir = Dir::new();
        let (ledger, ladder) = ledger_and_ladder();
        let caps = fbc_conformance::toy::caps().exec.unwrap().order;
        let mut link = Link::new(registry(), ledger, caps, ladder);
        let session = session();
        let orders = session.0.orders();
        let conn = |state| ExecEvent::Conn {
            stream: StreamId(0),
            state,
        };
        for (n, body) in (0u64..).zip([
            conn(ConnState::Open),
            conn(ConnState::Authenticated),
            ExecEvent::ResyncBegin {
                watermark: WallNs(1_000),
            },
            ExecEvent::ResyncEnd,
        ]) {
            link.on_exec(env(n, 1_000 + n, body));
        }
        let symbol =
            dispatch_market_data(&fbc_conformance::toy::caps(), |s| s.venue_symbol(SYMBOL_A))
                .unwrap();
        let market = MarketLease::acquire(&dir.0, "toy", "test-acct", &symbol).unwrap();
        let account = AccountLease::acquire(&dir.0, "toy", "test-acct").unwrap();
        link.reg()
            .start(INST_A, Leases::market(market).with_account(account))
            .unwrap();
        let lease = NamespaceLease::acquire(&dir.0, ACCT, OWN_NS).unwrap();
        let mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
        Rig {
            link,
            orders,
            mint,
            seq: 10,
            _session: session,
            _dir: dir,
        }
    }

    /// Hands the link `body` as the next input.
    fn input(&mut self, body: ExecEvent) {
        self.seq += 1;
        self.link.on_exec(env(self.seq, 2_000 + self.seq, body));
    }

    /// One post-only buy of 3 lots at 100 ticks on `INST_A`, authorized, submitted, sent,
    /// accepted and shown open as venue order `v`: its client id.
    fn placed(&mut self, v: &str) -> ClientOrderId {
        let cid = self.mint.mint().unwrap();
        let new = NewOrder {
            cid,
            inst: INST_A,
            side: Side::Buy,
            kind: OrderKind::Limit { px: Ticks(100) },
            qty: Lots::new(3).unwrap(),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let cmd = self.link.reg().place(new).unwrap();
        let auth = self.link.reg().authorize(ACCT, cmd).unwrap();
        let rpc: RpcId = self.link.submit(&self.orders, auth).unwrap();
        self.link.on_submitted(SubmitHandle {
            rpc,
            receipt: Ok(EncodeReceipt::new()),
        });
        let item = ItemRef {
            idx: 0,
            cid: Some(cid),
            vid: Some(vid(v)),
        };
        let outcome = SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        };
        self.input(ExecEvent::Outcome {
            rpc,
            item: Some(item),
            outcome,
        });
        self.input(update(cid, v, VenueOrderState::Open, 0));
        cid
    }

    /// The check over the rig's registry with the inventory's position on `INST_A`.
    fn check(
        &self,
        streamed: Option<SignedLots>,
        watched: &[ClientOrderId],
        ended: &mut Vec<ClientOrderId>,
    ) -> Option<Unreconciled> {
        let registry = self.link.registry();
        let inventory = registry.inventory(INST_A);
        unreconciled(registry, streamed, inventory, watched, ended)
    }

    /// Whether the registry holds our order `cid` with its fills read behind the venue's.
    fn behind(&self, cid: ClientOrderId) -> bool {
        fills_behind(self.link.registry().get(cid).unwrap())
    }
}

/// The fills report of our order `cid`: `venue` lots reported on an order event, `read` read.
fn fills(cid: ClientOrderId, venue: i64, read: i64) -> Option<Unreconciled> {
    Some(Unreconciled::Fills {
        cid,
        venue: Lots::new(venue).unwrap(),
        read: Lots::new(read).unwrap(),
    })
}

#[test]
fn a_streamed_position_other_than_the_inventory_differs_and_none_streamed_yet_agrees() {
    let registry = Registry::new();
    let flat = SignedLots(0);
    let mut ended = Vec::new();

    // None streamed yet, or the inventory's own: they agree.
    assert_eq!(unreconciled(&registry, None, flat, &[], &mut ended), None);
    let long = SignedLots(12);
    assert_eq!(
        unreconciled(&registry, Some(long), long, &[], &mut ended),
        None
    );

    // A long the inventory does not hold, and a short against a long: each differs, and says
    // both positions as passed, every time it is asked.
    for _ in 0..2 {
        assert_eq!(
            unreconciled(&registry, Some(long), flat, &[], &mut ended),
            Some(Unreconciled::Position {
                streamed: long,
                inventory: flat
            })
        );
    }
    assert_eq!(
        unreconciled(&registry, Some(SignedLots(-3)), long, &[], &mut ended),
        Some(Unreconciled::Position {
            streamed: SignedLots(-3),
            inventory: long
        })
    );
}

#[test]
fn an_order_whose_venue_fill_is_above_the_fills_read_differs_until_they_agree() {
    let mut rig = Rig::started();
    let a = rig.placed("v-a");
    let mut ended = Vec::new();
    assert!(!rig.behind(a));
    assert_eq!(rig.check(None, &[a], &mut ended), None, "nothing filled");

    // The order stream reports 2 lots filled; the fill stream has brought none.
    rig.input(update(a, "v-a", VenueOrderState::Open, 2));
    assert!(rig.behind(a));
    for _ in 0..2 {
        assert_eq!(rig.check(None, &[a], &mut ended), fills(a, 2, 0));
    }

    // One fill read: still behind. The second: they agree, the inventory 2 lots long.
    rig.input(fill(a, "f-1"));
    assert_eq!(rig.check(None, &[a], &mut ended), fills(a, 2, 1));
    rig.input(fill(a, "f-2"));
    assert!(!rig.behind(a));
    assert_eq!(rig.link.registry().inventory(INST_A), SignedLots(2));
    assert_eq!(rig.check(None, &[a], &mut ended), None);

    // A streamed position the inventory does not hold is reported before an order's fills.
    rig.input(update(a, "v-a", VenueOrderState::Open, 3));
    assert_eq!(rig.check(None, &[a], &mut ended), fills(a, 3, 2));
    assert_eq!(
        rig.check(Some(SignedLots(3)), &[a], &mut ended),
        Some(Unreconciled::Position {
            streamed: SignedLots(3),
            inventory: SignedLots(2)
        })
    );
    rig.input(fill(a, "f-3"));
    assert_eq!(rig.check(Some(SignedLots(3)), &[a], &mut ended), None);
}

#[test]
fn the_watched_orders_come_first_then_the_ended_ones_each_dropped_once_not_behind() {
    let mut rig = Rig::started();
    let a = rig.placed("v-a");
    let b = rig.placed("v-b");
    let c = rig.placed("v-c");
    let d = rig.placed("v-d");
    // A client id the registry never held.
    let unknown = rig.mint.mint().unwrap();

    // `a` rests behind; `b` and `d` ended filled whole, with no fill read; `c` rests with
    // nothing filled.
    rig.input(update(a, "v-a", VenueOrderState::Open, 1));
    rig.input(update(b, "v-b", VenueOrderState::Filled, 3));
    rig.input(update(d, "v-d", VenueOrderState::Filled, 3));
    assert!(rig.link.registry().get(b).unwrap().state().is_terminal());
    let watched = [unknown, c, a];
    let mut ended = vec![unknown, b, c, d];

    // The watched order behind comes first; the ended ones not behind, or not held, leave the
    // list, the others keeping their order.
    assert_eq!(rig.check(None, &watched, &mut ended), fills(a, 1, 0));
    assert_eq!(ended, [b, d]);

    // Once `a` agrees, the first ended one behind.
    rig.input(fill(a, "f-a"));
    assert_eq!(rig.check(None, &watched, &mut ended), fills(b, 3, 0));
    rig.input(fill(b, "f-b1"));
    rig.input(fill(b, "f-b2"));
    assert_eq!(rig.check(None, &watched, &mut ended), fills(b, 3, 2));
    assert_eq!(ended, [b, d]);

    // `b`'s last fill read: it leaves the list, and `d` is next.
    rig.input(fill(b, "f-b3"));
    assert_eq!(rig.check(None, &watched, &mut ended), fills(d, 3, 0));
    assert_eq!(ended, [d]);

    // While a streamed position differs the list is left as it is, though `d` is no longer
    // behind; once they agree it is trimmed.
    for id in ["f-d1", "f-d2", "f-d3"] {
        rig.input(fill(d, id));
    }
    let inventory = rig.link.registry().inventory(INST_A);
    assert_eq!(inventory, SignedLots(7));
    let streamed = Some(SignedLots(9));
    assert_eq!(
        rig.check(streamed, &watched, &mut ended),
        Some(Unreconciled::Position {
            streamed: SignedLots(9),
            inventory
        })
    );
    assert_eq!(ended, [d]);
    assert_eq!(rig.check(Some(inventory), &watched, &mut ended), None);
    assert!(ended.is_empty());
}
