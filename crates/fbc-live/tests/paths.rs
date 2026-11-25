//! The link's paths a consumer's session walks that no sample's test reaches on every run: what it
//! notes for an event out of its place, a venue error after the login, an epoch the codec closed, a
//! request the link never submitted, an amend reported sent, a request reported twice, an outcome
//! naming no item of its request, a fill the registry refuses, an unprotected order it cannot
//! cancel (decision 0155 item 9), a place reported not sent (FBC-y7lq), which a sample's test
//! reaches only when it loses its connection at the right moment, and the cancels not ours that
//! make `Link::cancels_not_ours_seen` hold, through `Link::forget` too (FBC-21g6, decision 0155
//! item 2), and the requests answered in full that `Link::forget` drops with no call to
//! `Link::release`, at the forget after the one that took their last note, so a sweep still
//! reads the notes a forget handed over against them, and those it keeps: an item unanswered,
//! or answered provisionally or `Unknown` while its order lives (FBC-mu68, decision 0155 items 2
//! and 5).
//! Every command goes through an fbc-oms authorization and a real `ExecSession`'s `ExecOrders`,
//! on the conformance toy, the session never run: its submissions are queued, and the test hands
//! the link what the session would report.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_conformance::toy::{EXEC_URL_KEY, INST_A, OWN_NS, REST_URL_KEY, SYMBOL_A, ToyFactory};
use fbc_core::{
    AccountKey, AccountLease, AckLevel, AssetSym, CancelReason, Channel, CidMatch, CidMint,
    ClientOrderId, ConnKey, ConnState, EncodeReceipt, Envelope, ExchNs, ExchTsKind, ExecEvent,
    FillEvent, FillIdent, FillOrigin, ItemRef, Liquidity3, Lots, MarketLease, ModeScope, MonoNs,
    Namespace, NamespaceLease, NewOrder, NonceBlock, NonceSource, NotSentReason, OrderCaps,
    OrderKind, OrderUpdate, RefKind, Reject, RejectKind, RpcId, Secrets, Side, SignedLots, Stamp,
    StreamId, SubmitHandle, SubmitOutcome, TagSet, Ticks, Tif, VenueCaps, VenueConfig,
    VenueFactory, VenueMode, VenueOrderId, VenueOrderSnapshot, VenueOrderState, WallNs,
    dispatch_market_data,
};
use fbc_live::link::{CODEC_CLOSED, Link, LinkHandler, Note};
use fbc_live::sweep::{RateHold, Sweep, SweepEnd, SweepState};
use fbc_oms::{
    CancelChoice, CancelEverything, FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig,
    MarketCapsConfig, PreTradeCaps, Registry,
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

/// The toy's capabilities.
fn toy() -> VenueCaps {
    fbc_conformance::toy::caps()
}

/// The toy's order caps.
fn order_caps() -> OrderCaps {
    toy().exec.unwrap().order
}

/// A directory removed when it is dropped.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Dir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("fbc-live-paths-test-{}-{n}", std::process::id()));
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

/// An order-entry session on the toy for `ACCT`, handing its events to `handler`, never run:
/// its URLs are loopback ports nothing listens on.
fn session(handler: LinkHandler) -> (ExecSession<LinkHandler>, ExecControl) {
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, "ws://127.0.0.1:9/");
    cfg.insert(REST_URL_KEY, "http://127.0.0.1:9");
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

/// A rejection of kind Other with no code.
fn reject(raw: &str) -> Reject {
    Reject {
        kind: RejectKind::Other,
        venue_code: None,
        raw: raw.into(),
    }
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

/// Authenticated, then a resync showing nothing, from ingest sequence `seq` at `at`.
fn authenticated_and_resynced(link: &mut Link, seq: u64, at: u64) {
    for (n, body) in (0u64..).zip([
        conn(ConnState::Open),
        conn(ConnState::Authenticated),
        ExecEvent::ResyncBegin {
            watermark: WallNs(i64::try_from(at).unwrap()),
        },
        ExecEvent::ResyncEnd,
    ]) {
        link.on_exec(env(seq + n, at + n, body));
    }
}

/// A link over `reg` under `caps`, beside a session on the toy whose `ExecOrders` it submits
/// through, a market lease and an account lease on `INST_A` (held by the registry once
/// started), and a mint under the toy's own namespace.
struct Rig {
    link: Link,
    orders: ExecOrders,
    mint: CidMint,
    _session: (ExecSession<LinkHandler>, ExecControl),
    _dir: Dir,
}

impl Rig {
    /// The link over `registry()` under `caps`, authenticated, resynced flat and Started on
    /// `INST_A`.
    fn started(caps: OrderCaps) -> Rig {
        let dir = Dir::new();
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
        let mut link = Link::new(registry(), ledger, caps, ladder);
        // The session's own handler holds a link of its own: it is never run, so nothing
        // reaches it.
        let session = session(LinkHandler {
            link: Rc::new(RefCell::new(link_over(Registry::new()))),
            wake: Box::new(|| {}),
        });
        let orders = session.0.orders();
        authenticated_and_resynced(&mut link, 0, 1_000);
        let symbol = dispatch_market_data(&toy(), |s| s.venue_symbol(SYMBOL_A)).unwrap();
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
            _session: session,
            _dir: dir,
        }
    }

    /// One post-only buy of a lot at 100 ticks on `INST_A`, authorized and submitted: its
    /// client id and request id.
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
        let cmd = self.link.reg().place(new).unwrap();
        let auth = self.link.reg().authorize(ACCT, cmd).unwrap();
        (cid, self.link.submit(&self.orders, auth).unwrap())
    }
}

/// Our order `cid` shown open as venue order `v` by its order event.
fn open(cid: ClientOrderId, v: &str) -> ExecEvent {
    ExecEvent::Order(OrderUpdate {
        cid: Some(CidMatch::Ours(cid)),
        vid: Some(vid(v)),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Open,
        cum_filled: Lots::ZERO,
        px: Some(Ticks(100)),
        qty: Some(Lots::new(1).unwrap()),
        post_only: Some(true),
        reduce_only: Some(false),
    })
}

/// A resync's or the session's snapshot of venue order `v` open on `INST_A`, naming `cid`.
fn snapshot(cid: Option<CidMatch>, v: &str) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid,
        vid: vid(v),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(100)),
        qty: Lots::new(1).unwrap(),
        cum_filled: Lots::ZERO,
        post_only: None,
        reduce_only: None,
    }
}

/// A buy fill of `qty` lots of our order `cid` on `INST_A`, keyed by the venue's fill id `id`.
fn fill(cid: ClientOrderId, id: &str, qty: i64) -> FillEvent {
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
        qty: Lots::new(qty).unwrap(),
        liquidity: Liquidity3::Maker,
        fee: dispatch_market_data(&caps, |s| s.fee(0, AssetSym::new("USDC").unwrap())).unwrap(),
        realized_pnl: None,
        realized_funding: None,
        replay: false,
    }
}

#[test]
fn events_out_of_their_place_and_a_venue_error_after_the_login_are_noted_as_problems() {
    let mut link = link_over(Registry::new());

    // A resync's parts with no resync begun: each a problem, nothing collected.
    link.on_exec(env(0, 100, ExecEvent::ResyncEnd));
    link.on_exec(env(1, 101, ExecEvent::ResyncOrder(snapshot(None, "v-out"))));
    link.on_exec(env(
        2,
        102,
        ExecEvent::ResyncPosition {
            inst: INST_A,
            qty: SignedLots(1),
            avg_entry: None,
        },
    ));
    assert_eq!(
        problems(&link),
        [
            "ResyncEnd with no ResyncBegin",
            "ResyncOrder outside a resync",
            "ResyncPosition outside a resync",
        ]
    );

    // A venue mode moves nothing the link holds and notes nothing.
    let held = link.notes().len();
    link.on_exec(env(
        3,
        103,
        ExecEvent::Mode {
            scope: ModeScope::Account,
            mode: VenueMode::Normal,
        },
    ));
    assert_eq!(link.notes().len(), held);

    // A venue error naming no request after the login is a problem, not a refused login, and
    // the epoch's end says it.
    link.on_exec(env(4, 104, conn(ConnState::Authenticated)));
    link.on_exec(env(
        5,
        105,
        ExecEvent::UncorrelatedError(reject("the venue is busy")),
    ));
    assert_eq!(
        problems(&link).last().map(String::as_str),
        Some("venue error naming no request: Other, the venue is busy")
    );
    assert_eq!(link.login_refusals(), None);
    link.on_epoch_end(KEY);
    assert!(matches!(
        link.notes().last(),
        Some(Note::EpochEnd { key, why }) if *key == KEY && why == "the venue is busy"
    ));

    // An epoch the codec closed, with no venue error, ends saying so.
    let next = ConnKey { conn: 0, epoch: 1 };
    link.on_exec(env(6, 106, conn(ConnState::Closed)));
    link.on_epoch_end(next);
    assert!(matches!(
        link.notes().last(),
        Some(Note::EpochEnd { key, why }) if *key == next && why == CODEC_CLOSED
    ));

    // Orders told unprotected that are not ours: none told, nothing cancelled, nothing left.
    let foreign = CidMatch::Foreign(Namespace::new(9));
    link.on_unprotected(&[
        snapshot(None, "v-none"),
        snapshot(Some(foreign), "v-foreign"),
    ]);
    assert!(matches!(
        link.notes().last(),
        Some(Note::Unprotected { told, rpcs, left })
            if told.is_empty() && rpcs.is_empty() && left.is_empty()
    ));

    // A request the link never submitted, reported sent: noted, counted as neither a place
    // nor a cancel.
    link.on_submitted(sent(RpcId(99)));
    assert!(matches!(
        link.notes().last(),
        Some(Note::Submitted {
            rpc: RpcId(99),
            sent: Ok(())
        })
    ));
    assert_eq!(link.sent_counts(), (0, 0));
    assert_eq!(problems(&link).len(), 4);
}

#[test]
fn what_the_registry_refuses_of_a_submitted_requests_reports_is_a_problem() {
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    let link = &mut rig.link;

    // Reported sent twice, at two instants: the registry keeps the first.
    link.on_submitted(sent(place));
    link.on_submitted(sent(place));
    assert_eq!(
        problems(link),
        [format!("request {}: SentRecorded({a:?})", place.0)]
    );

    // Accepted and shown open; then an answer naming another order for its item, and one for
    // an item the request does not hold, which names nothing.
    link.on_exec(env(
        10,
        2_000,
        ExecEvent::Outcome {
            rpc: place,
            item: Some(ItemRef {
                idx: 0,
                cid: Some(a),
                vid: Some(vid("v-a")),
            }),
            outcome: SubmitOutcome::Accepted {
                ack: AckLevel::Final,
            },
        },
    ));
    link.on_exec(env(11, 2_001, open(a, "v-a")));
    let other = rig.mint.mint().unwrap();
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    for (seq, item) in [
        (
            12,
            ItemRef {
                idx: 0,
                cid: Some(other),
                vid: None,
            },
        ),
        (
            13,
            ItemRef {
                idx: 3,
                cid: None,
                vid: None,
            },
        ),
    ] {
        link.on_exec(env(
            seq,
            2_000 + seq,
            ExecEvent::Outcome {
                rpc: place,
                item: Some(item),
                outcome: accepted.clone(),
            },
        ));
    }
    assert_eq!(
        problems(link)[1..],
        [format!(
            "request {}: ItemNamesAnother {{ cid: {a:?}, item: {other:?} }}",
            place.0
        )]
    );
    assert!(matches!(
        link.notes().last(),
        Some(Note::Outcome { rpc, item: Some(3), ours: true, .. }) if *rpc == place
    ));
    assert!(
        link.registry()
            .get(a)
            .is_some_and(|r| !r.state().is_terminal())
    );

    // An amend reported sent is counted as neither a place nor a cancel; a cancel is.
    let caps = order_caps();
    let amend = rig
        .link
        .reg()
        .live(a)
        .unwrap()
        .amend(&caps, Ticks(99), Lots::new(1).unwrap(), false)
        .unwrap();
    let auth = rig.link.reg().authorize(ACCT, amend).unwrap();
    let amend = rig.link.submit(&rig.orders, auth).unwrap();
    assert_eq!(rig.link.sent_counts(), (2, 0));
    rig.link.on_submitted(sent(amend));
    assert_eq!(rig.link.sent_counts(), (2, 0));
    let cancel = match rig.link.reg().cancellable(a).unwrap().cancel(&caps) {
        CancelChoice::Send(cmd) => cmd,
        CancelChoice::AwaitAck => panic!("the toy cancels by client id"),
    };
    let auth = rig.link.reg().authorize(ACCT, cancel).unwrap();
    let cancel = rig.link.submit(&rig.orders, auth).unwrap();
    rig.link.on_submitted(sent(cancel));
    assert_eq!(rig.link.sent_counts(), (2, 1));

    // Told unprotected with no session attached: its Safety cancel is built but not submitted.
    rig.link
        .on_unprotected(&[snapshot(Some(CidMatch::Ours(a)), "v-a")]);
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Unprotected { told, rpcs, left })
            if told == &[(a, INST_A)]
                && rpcs.is_empty()
                && left == &["a Safety cancel was not submitted: no session attached"]
    ));

    // Fills of the order past what a count holds: the registry refuses the second, which
    // counts nothing and is unexplained.
    rig.link
        .on_exec(env(20, 3_000, ExecEvent::Fill(fill(a, "f-1", i64::MAX))));
    rig.link
        .on_exec(env(21, 3_001, ExecEvent::Fill(fill(a, "f-2", 1))));
    let notes = rig.link.notes();
    assert!(matches!(
        &notes[notes.len() - 2],
        Note::Fill {
            unexplained: false,
            ours: true,
            ..
        }
    ));
    assert!(matches!(
        &notes[notes.len() - 1],
        Note::Fill { seen, unexplained: true, ours: false }
            if seen == &format!("refused by the registry: FillOverflow({INST_A:?})")
    ));
}

#[test]
fn an_unprotected_order_whose_cancel_waits_for_its_ack_is_left_held() {
    // A venue whose cancels name the venue's id only, and which batches none: an order not yet
    // acknowledged has no reference a cancel can name.
    let mut caps = order_caps();
    caps.cancel_refs = TagSet::of(&[RefKind::Venue]);
    caps.batch_cancel = None;
    let mut rig = Rig::started(caps);
    let (a, place) = rig.place();
    rig.link.on_submitted(sent(place));
    rig.link
        .on_unprotected(&[snapshot(Some(CidMatch::Ours(a)), "v-a")]);
    let left = format!(
        "client id sequence {}: no cancel built: awaits its ack",
        a.seq()
    );
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Unprotected { told, rpcs, left: l })
            if told == &[(a, INST_A)] && rpcs.is_empty() && l == &[left]
    ));
    assert_eq!(rig.link.safety_cancel_of(a), None);
}

#[test]
fn a_place_reported_not_sent_tells_its_order_so_and_is_read_as_its_outcome() {
    let mut rig = Rig::started(order_caps());
    let (cid, rpc) = rig.place();
    assert_eq!(rig.link.outcome_of(rpc), None, "unanswered");
    let not_sent = SubmitHandle {
        rpc,
        receipt: Err(NotSentReason::Disconnected),
    };
    rig.link.on_submitted(not_sent);
    let outcome = SubmitOutcome::NotSent(NotSentReason::Disconnected);
    assert_eq!(rig.link.outcome_of(rpc), Some(outcome));
    let rec = rig.link.registry().get(cid).unwrap();
    assert!(rec.state().is_terminal(), "{:?}", rec.state());
    assert_eq!(rec.sent_at(), None);
    assert_eq!(rig.link.sent_counts(), (0, 0));
    assert_eq!(problems(&rig.link), Vec::<String>::new());
}

/// Our order `cid`, venue order `v`, ended by a user cancel (`Canceled(Requested)`), which a
/// venue may report without saying who sent it.
fn user_cancelled(cid: ClientOrderId, v: &str) -> ExecEvent {
    match open(cid, v) {
        ExecEvent::Order(u) => ExecEvent::Order(OrderUpdate {
            state: VenueOrderState::Canceled(CancelReason::Requested),
            ..u
        }),
        _ => unreachable!("open builds an order event"),
    }
}

/// Our place `rpc` of order `cid` accepted as venue order `v` and shown open, from ingest
/// sequence `seq`.
fn accepted_and_open(link: &mut Link, rpc: RpcId, cid: ClientOrderId, v: &str, seq: u64) {
    link.on_submitted(sent(rpc));
    let item = ItemRef {
        idx: 0,
        cid: Some(cid),
        vid: Some(vid(v)),
    };
    let outcome = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    link.on_exec(env(
        seq,
        2_000 + seq,
        ExecEvent::Outcome {
            rpc,
            item: Some(item),
            outcome,
        },
    ));
    link.on_exec(env(seq + 1, 2_001 + seq, open(cid, v)));
}

/// The cancels not ours the link noted, by client id, in order.
fn not_ours(link: &Link) -> Vec<ClientOrderId> {
    link.notes()
        .iter()
        .filter_map(|n| match n {
            Note::CancelNotOurs { cid, .. } => Some(*cid),
            _ => None,
        })
        .collect()
}

#[test]
fn cancels_not_ours_seen_holds_from_the_first_cancel_not_ours_on_and_through_forget() {
    // A user cancel no cancel of ours covers: noted at once, and seen from then on.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    assert!(!rig.link.cancels_not_ours_seen(), "none noted yet");
    rig.link.on_exec(env(12, 2_100, user_cancelled(a, "v-a")));
    assert_eq!(not_ours(&rig.link), [a]);
    assert!(rig.link.cancels_not_ours_seen());
    // Forgetting every note, the cancel not ours among them, keeps it, as does what follows.
    let taken = rig.link.forget(rig.link.notes().len());
    assert!(
        taken
            .iter()
            .any(|n| matches!(n, Note::CancelNotOurs { cid, .. } if *cid == a))
    );
    assert!(rig.link.notes().is_empty());
    assert!(rig.link.cancels_not_ours_seen());
    let (b, place) = rig.place();
    accepted_and_open(&mut rig.link, place, b, "v-b", 20);
    rig.link.forget(rig.link.notes().len());
    assert!(rig.link.cancels_not_ours_seen(), "never false again");

    // Codex r4310042612: a user cancel while only an unanswered cancel of ours covers the order
    // is held, and nothing is seen; the venue refusing that cancel makes it a cancel not ours,
    // seen from then on, through forget too.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let caps = order_caps();
    let cancel = match rig.link.reg().cancellable(a).unwrap().cancel(&caps) {
        CancelChoice::Send(cmd) => cmd,
        CancelChoice::AwaitAck => panic!("the toy cancels by client id"),
    };
    let auth = rig.link.reg().authorize(ACCT, cancel).unwrap();
    let cancel = rig.link.submit(&rig.orders, auth).unwrap();
    rig.link.on_submitted(sent(cancel));
    rig.link.on_exec(env(12, 2_100, user_cancelled(a, "v-a")));
    assert!(not_ours(&rig.link).is_empty(), "held");
    assert!(!rig.link.cancels_not_ours_seen(), "held, not seen");
    rig.link.on_exec(env(
        13,
        2_101,
        ExecEvent::Outcome {
            rpc: cancel,
            item: Some(ItemRef {
                idx: 0,
                cid: Some(a),
                vid: Some(vid("v-a")),
            }),
            outcome: SubmitOutcome::Rejected(reject("no such order")),
        },
    ));
    assert_eq!(not_ours(&rig.link), [a]);
    assert!(rig.link.cancels_not_ours_seen());
    rig.link.forget(rig.link.notes().len());
    assert!(rig.link.cancels_not_ours_seen(), "kept through forget");
}

// FBC-mu68 (decision 0155 items 2 and 5): the link drops a request answered in full at the
// `Link::forget` after the one that took its last note, with no call to `Link::release`.

/// Our place `rpc`'s item `idx` (order `cid`) answered `outcome`, as ingest sequence `seq`.
fn answer_item(
    link: &mut Link,
    seq: u64,
    rpc: RpcId,
    idx: u16,
    cid: ClientOrderId,
    outcome: SubmitOutcome,
) {
    let item = ItemRef {
        idx,
        cid: Some(cid),
        vid: None,
    };
    let body = ExecEvent::Outcome {
        rpc,
        item: Some(item),
        outcome,
    };
    link.on_exec(env(seq, 2_000 + seq, body));
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// Every note forgotten twice: the first takes them, the second drops each request answered in
/// full whose notes the first took.
fn forget_twice(link: &mut Link) {
    link.forget(link.notes().len());
    link.forget(0);
}

impl Rig {
    /// A batch of `n` post-only buys of a lot at 100 ticks on `INST_A`, authorized and
    /// submitted: their client ids and the request id.
    fn place_batch(&mut self, n: usize) -> (Vec<ClientOrderId>, RpcId) {
        let cids: Vec<ClientOrderId> = (0..n).map(|_| self.mint.mint().unwrap()).collect();
        let orders = cids
            .iter()
            .map(|cid| NewOrder {
                cid: *cid,
                inst: INST_A,
                side: Side::Buy,
                kind: OrderKind::Limit { px: Ticks(100) },
                qty: Lots::new(1).unwrap(),
                tif: Tif::Gtc,
                channel: Channel::Public,
                post_only: true,
                reduce_only: false,
                reducing: false,
            })
            .collect();
        let plan = self.link.reg().place_batch(orders).unwrap();
        let auth = self
            .link
            .reg()
            .authorize(ACCT, plan.command.unwrap())
            .unwrap();
        (cids, self.link.submit(&self.orders, auth).unwrap())
    }

    /// A cancel of our order `cid`, authorized and submitted: its request id.
    fn cancel(&mut self, cid: ClientOrderId) -> RpcId {
        let caps = order_caps();
        let cancel = match self.link.reg().cancellable(cid).unwrap().cancel(&caps) {
            CancelChoice::Send(cmd) => cmd,
            CancelChoice::AwaitAck => panic!("the toy cancels by client id"),
        };
        let auth = self.link.reg().authorize(ACCT, cancel).unwrap();
        self.link.submit(&self.orders, auth).unwrap()
    }
}

#[test]
fn a_request_answered_in_full_is_dropped_by_the_forget_after_the_one_that_took_its_notes() {
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let cancel = rig.cancel(a);
    rig.link.on_submitted(sent(cancel));
    answer_item(&mut rig.link, 12, cancel, 0, a, accepted());

    // Answered, its notes left: outcome_of answers each, the cancel reads as one on its market.
    assert_eq!(rig.link.requests_held(), 2);
    assert_eq!(rig.link.outcome_of(place), Some(accepted()));
    assert_eq!(rig.link.outcome_of(cancel), Some(accepted()));
    assert!(rig.link.is_cancel_on(cancel, INST_A));

    // The forget that takes their last notes keeps them, so the consumer reads those notes
    // against the link as it was; outcome_of sees only the notes left, as before.
    let taken = rig.link.forget(rig.link.notes().len());
    assert!(!taken.is_empty());
    assert_eq!(rig.link.requests_held(), 2);
    assert!(rig.link.is_cancel_on(cancel, INST_A));
    assert_eq!(rig.link.cids_of(cancel), [a]);
    assert_eq!(rig.link.outcome_of(place), None);

    // The next forget drops both, with no call to release.
    rig.link.forget(0);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(!rig.link.is_cancel_on(cancel, INST_A));
    assert!(rig.link.cids_of(cancel).is_empty());
    assert_eq!(rig.link.sent_counts(), (1, 1));
    assert_eq!(problems(&rig.link), Vec::<String>::new());
}

#[test]
fn a_batch_answered_item_by_item_is_dropped_once_its_notes_are_forgotten() {
    let mut rig = Rig::started(order_caps());
    let (cids, batch) = rig.place_batch(2);
    rig.link.on_submitted(sent(batch));
    answer_item(&mut rig.link, 10, batch, 0, cids[0], accepted());
    assert_eq!(rig.link.outcome_of(batch), None, "an item unanswered");
    answer_item(&mut rig.link, 11, batch, 1, cids[1], accepted());
    assert_eq!(rig.link.outcome_of(batch), Some(accepted()));
    assert_eq!(rig.link.cids_of(batch), cids);
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(rig.link.cids_of(batch).is_empty());
}

#[test]
fn a_whole_request_reject_and_a_not_sent_report_each_answer_a_request_in_full() {
    let mut rig = Rig::started(order_caps());
    // A batch refused whole, by an outcome naming no item.
    let (_, batch) = rig.place_batch(2);
    rig.link.on_submitted(sent(batch));
    let refused = SubmitOutcome::Rejected(reject("refused"));
    let body = ExecEvent::Outcome {
        rpc: batch,
        item: None,
        outcome: refused.clone(),
    };
    rig.link.on_exec(env(10, 2_010, body));
    // A place reported not sent.
    let (_, place) = rig.place();
    rig.link.on_submitted(SubmitHandle {
        rpc: place,
        receipt: Err(NotSentReason::Disconnected),
    });
    assert_eq!(rig.link.outcome_of(batch), Some(refused));
    let not_sent = SubmitOutcome::NotSent(NotSentReason::Disconnected);
    assert_eq!(rig.link.outcome_of(place), Some(not_sent.clone()));
    assert_eq!(rig.link.requests_held(), 2);
    // A forget that takes no note drops neither: the not-sent report is the place's one note.
    rig.link.forget(0);
    assert_eq!(rig.link.requests_held(), 2);
    assert_eq!(rig.link.outcome_of(place), Some(not_sent));
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
}

#[test]
fn a_request_with_an_item_unanswered_is_kept_through_a_forget() {
    let mut rig = Rig::started(order_caps());
    let (cids, batch) = rig.place_batch(2);
    rig.link.on_submitted(sent(batch));
    answer_item(&mut rig.link, 10, batch, 0, cids[0], accepted());
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 1, "item 1 unanswered");
    assert_eq!(rig.link.cids_of(batch), cids);

    // Item 1's answer still routes as ours, and the request goes once its note is forgotten.
    answer_item(&mut rig.link, 11, batch, 1, cids[1], accepted());
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Outcome { rpc, item: Some(1), ours: true, .. }) if *rpc == batch
    ));
    rig.link.forget(rig.link.notes().len());
    assert_eq!(
        rig.link.requests_held(),
        1,
        "its last note taken by this forget"
    );
    rig.link.forget(0);
    assert_eq!(rig.link.requests_held(), 0);
}

fn provisional() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Provisional,
    }
}

#[test]
fn a_place_accepted_provisionally_is_kept_until_its_final_acceptance_routes_as_ours() {
    // A two-phase venue's place (decision 0092): its provisional acceptance is not an answer for
    // good, so the request is kept through forgets while its order lives, and its final
    // acceptance is ours. A later answer to the request once dropped is the session's own.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    rig.link.on_submitted(sent(place));
    answer_item(&mut rig.link, 10, place, 0, a, provisional());
    rig.link.on_exec(env(11, 2_011, open(a, "v-a")));
    forget_twice(&mut rig.link);
    assert_eq!(
        rig.link.requests_held(),
        1,
        "its order lives, provisionally accepted"
    );

    answer_item(&mut rig.link, 12, place, 0, a, accepted());
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Outcome { rpc, ours: true, .. }) if *rpc == place
    ));
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);

    answer_item(&mut rig.link, 13, place, 0, a, accepted());
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Outcome { rpc, ours: false, .. }) if *rpc == place
    ));
    assert_eq!(rig.link.outcome_of(place), None);
    assert_eq!(rig.link.requests_held(), 0);
    assert_eq!(problems(&rig.link), Vec::<String>::new());
}

#[test]
fn an_amend_accepted_provisionally_is_kept_until_its_final_acceptance_reaches_the_registry() {
    // fbc-oms reads an amend answered only at its final acceptance: the link keeps the amend
    // through forgets after its provisional one, so the final one reaches the registry.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let caps = order_caps();
    let amend = rig
        .link
        .reg()
        .live(a)
        .unwrap()
        .amend(&caps, Ticks(99), Lots::new(1).unwrap(), false)
        .unwrap();
    let auth = rig.link.reg().authorize(ACCT, amend).unwrap();
    let amend = rig.link.submit(&rig.orders, auth).unwrap();
    rig.link.on_submitted(sent(amend));
    answer_item(&mut rig.link, 12, amend, 0, a, provisional());
    forget_twice(&mut rig.link);
    assert_eq!(
        rig.link.requests_held(),
        1,
        "the place dropped, the amend kept"
    );
    assert!(rig.link.registry().get(a).unwrap().amend_unconfirmed());

    answer_item(&mut rig.link, 13, amend, 0, a, accepted());
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Outcome { rpc, ours: true, .. }) if *rpc == amend
    ));
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert_eq!(problems(&rig.link), Vec::<String>::new());
}

#[test]
fn a_cancel_answered_unknown_is_kept_until_its_order_ends() {
    // An answer that is not for good (`Unknown`, or a provisional acceptance) keeps the request
    // while its order lives; once the order has ended, that answer is enough.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let cancel = rig.cancel(a);
    rig.link.on_submitted(sent(cancel));
    answer_item(&mut rig.link, 12, cancel, 0, a, SubmitOutcome::Unknown);
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 1, "the cancel's order lives");
    assert!(rig.link.is_cancel_on(cancel, INST_A));

    // Ended by the venue's own cancel: an end by a user cancel is held on the cancel, and the
    // cancel kept until it is answered (below, Codex on PR #303).
    rig.link.on_exec(env(13, 2_013, venue_cancelled(a, "v-a")));
    assert!(rig.link.registry().get(a).unwrap().state().is_terminal());
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(!rig.link.is_cancel_on(cancel, INST_A));
}

#[test]
fn an_answer_naming_an_item_past_the_requests_items_answers_none_of_them() {
    // A one-item place answered for an item it does not have: its one item is still unanswered,
    // so the request is kept and that item's answer, when it comes, reaches the registry.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    rig.link.on_submitted(sent(place));
    answer_item(&mut rig.link, 10, place, 1, a, accepted());
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 1, "item 0 unanswered");

    answer_item(&mut rig.link, 11, place, 0, a, accepted());
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Outcome { rpc, item: Some(0), ours: true, .. }) if *rpc == place
    ));
    assert!(rig.link.registry().get(a).unwrap().acknowledged());
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
}

#[test]
fn a_cancel_all_is_dropped_once_answered_provisionally() {
    // A request naming no order: fbc-oms reads none of its answers, so its first is enough.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    rig.link.reg().kill(INST_A);
    let built = rig.link.reg().cancel_everything(INST_A, &order_caps());
    let CancelEverything::CancelAll { command, .. } = built else {
        panic!("no cancel-all: {built:?}");
    };
    let auth = rig.link.reg().authorize(ACCT, command).unwrap();
    let all = rig.link.submit(&rig.orders, auth).unwrap();
    rig.link.on_submitted(sent(all));
    let body = ExecEvent::Outcome {
        rpc: all,
        item: Some(ItemRef {
            idx: 0,
            cid: None,
            vid: None,
        }),
        outcome: provisional(),
    };
    rig.link.on_exec(env(12, 2_012, body));
    assert!(rig.link.is_cancel_on(all, INST_A));
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(!rig.link.is_cancel_on(all, INST_A));
}

#[test]
fn sent_counts_counts_the_notes_of_requests_dropped_after_their_forget() {
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let (b, other) = rig.place();
    accepted_and_open(&mut rig.link, other, b, "v-b", 20);
    let cancel = rig.cancel(a);
    rig.link.on_submitted(sent(cancel));
    answer_item(&mut rig.link, 30, cancel, 0, a, accepted());
    assert_eq!(rig.link.sent_counts(), (2, 1));

    // Taken in two parts: the first place's notes, then the rest.
    rig.link.forget(3);
    assert_eq!(rig.link.sent_counts(), (2, 1));
    rig.link.forget(rig.link.notes().len());
    assert_eq!(rig.link.sent_counts(), (2, 1));
    rig.link.forget(0);
    assert_eq!(rig.link.requests_held(), 0);
    assert_eq!(rig.link.sent_counts(), (2, 1));
}

#[test]
fn release_still_drops_at_once_and_forget_drops_what_was_not_released() {
    let mut rig = Rig::started(order_caps());
    let (a, released) = rig.place();
    let (b, place) = rig.place();
    assert_eq!(rig.link.requests_held(), 2);
    // Released unanswered, with its notes left: gone at once, a later answer the session's own.
    rig.link.release(released);
    assert_eq!(rig.link.requests_held(), 1);
    rig.link.on_submitted(sent(released));
    answer_item(&mut rig.link, 10, released, 0, a, accepted());
    assert!(matches!(
        rig.link.notes().last(),
        Some(Note::Outcome { rpc, ours: false, .. }) if *rpc == released
    ));
    // The other, answered and forgotten, is dropped with no release.
    rig.link.on_submitted(sent(place));
    answer_item(&mut rig.link, 11, place, 0, b, accepted());
    assert_eq!(rig.link.requests_held(), 1);
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
}

#[test]
fn a_safety_cancel_answered_is_dropped_with_its_entry_once_its_notes_are_forgotten() {
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    rig.link.attach(rig.orders.clone(), ACCT);
    rig.link
        .on_unprotected(&[snapshot(Some(CidMatch::Ours(a)), "v-a")]);
    let safety = rig.link.safety_cancel_of(a).expect("a Safety cancel");
    rig.link.on_submitted(sent(safety));
    answer_item(&mut rig.link, 12, safety, 0, a, accepted());
    rig.link.forget(rig.link.notes().len());
    assert_eq!(rig.link.safety_cancel_of(a), Some(safety));
    assert!(rig.link.is_cancel_on(safety, INST_A));
    rig.link.forget(0);
    assert_eq!(rig.link.safety_cancel_of(a), None);
    assert!(!rig.link.is_cancel_on(safety, INST_A));
    assert_eq!(rig.link.requests_held(), 0);
}

#[test]
fn a_sweep_handed_a_rate_refusal_by_forget_reads_it_as_a_cancel_on_its_market() {
    // The consumer's cancel of the bid is refused for its rate, its last answer; the consumer
    // takes the notes with forget, records the refusal on its hold and hands each to the sweep
    // (as testnet_quote does). The link still says the request is a cancel on the market, so
    // the cancel of everything is due again once the hold passes.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let cancel = rig.cancel(a);
    rig.link.on_submitted(sent(cancel));
    rig.link.reg().kill(INST_A);
    let (mut sweep, _round) =
        Sweep::start(&mut rig.link, &rig.orders, ACCT, INST_A, SweepEnd::AtStop);
    let limited = Reject {
        kind: RejectKind::RateLimited { retry_after: None },
        venue_code: None,
        raw: "slow down".into(),
    };
    answer_item(
        &mut rig.link,
        12,
        cancel,
        0,
        a,
        SubmitOutcome::Rejected(limited),
    );
    let now = MonoNs(5_000);
    let mut hold = RateHold::new(Duration::from_secs(1)).unwrap();
    hold.refused(None, now);
    let taken = rig.link.forget(rig.link.notes().len());
    for note in &taken {
        sweep.observe(&rig.link, note);
    }
    let polled = sweep.poll(&mut rig.link, &rig.orders, &hold, now);
    assert_eq!(
        polled.state,
        SweepState::Waiting {
            resend_at: hold.until()
        }
    );
    assert!(hold.until().is_some());
    // The next forget drops the refused cancel: the consumer has read its notes.
    rig.link.forget(0);
    assert!(!rig.link.is_cancel_on(cancel, INST_A));
}

// Codex on PR #303: a request the link's coverage still waits on is kept through forgets, so its
// later answer routes as ours and reaches the coverage (Codex r4310042612 on PR #159).

/// Our order `cid`, venue order `v`, ended by the venue's own cancel (`Canceled(Venue)`), which
/// no cancel of ours is waited on for.
fn venue_cancelled(cid: ClientOrderId, v: &str) -> ExecEvent {
    match open(cid, v) {
        ExecEvent::Order(u) => ExecEvent::Order(OrderUpdate {
            state: VenueOrderState::Canceled(CancelReason::Venue),
            ..u
        }),
        _ => unreachable!("open builds an order event"),
    }
}

/// Whether a note left is an answer to request `rpc` routed as ours.
fn answered_as_ours(link: &Link, rpc: RpcId) -> bool {
    link.notes()
        .iter()
        .any(|n| matches!(n, Note::Outcome { rpc: r, ours, .. } if *r == rpc && *ours))
}

/// A cancel of our order `a`, sent and answered `Unknown`, then `a` ended by a user cancel the
/// link holds on that cancel, and the notes then forgotten by two rounds of `forget_twice`: the
/// rig, the order and the cancel.
fn held_on_an_unknown_cancel() -> (Rig, ClientOrderId, RpcId) {
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let cancel = rig.cancel(a);
    rig.link.on_submitted(sent(cancel));
    answer_item(&mut rig.link, 12, cancel, 0, a, SubmitOutcome::Unknown);
    rig.link.on_exec(env(13, 2_013, user_cancelled(a, "v-a")));
    assert!(rig.link.registry().get(a).unwrap().state().is_terminal());
    assert!(not_ours(&rig.link).is_empty(), "held on the cancel");
    forget_twice(&mut rig.link);
    forget_twice(&mut rig.link);
    assert_eq!(
        rig.link.requests_held(),
        1,
        "the place dropped, the cancel kept while the order's end is held on it"
    );
    assert_eq!(rig.link.cids_of(cancel), [a]);
    assert!(rig.link.is_cancel_on(cancel, INST_A));
    (rig, a, cancel)
}

#[test]
fn a_cancel_gone_unknown_is_kept_while_its_orders_end_is_held_and_its_refusal_is_noted() {
    // The venue refuses the cancel after the order's end: a cancel not ours, noted once the
    // refusal routes as ours, and the request dropped once its notes are forgotten.
    let (mut rig, a, cancel) = held_on_an_unknown_cancel();
    let refused = SubmitOutcome::Rejected(reject("no such order"));
    answer_item(&mut rig.link, 14, cancel, 0, a, refused);
    assert!(answered_as_ours(&rig.link, cancel));
    assert_eq!(not_ours(&rig.link), [a]);
    assert!(rig.link.cancels_not_ours_seen());
    assert_eq!(problems(&rig.link), Vec::<String>::new());
    forget_twice(&mut rig.link);
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(rig.link.cancels_not_ours_seen());
}

#[test]
fn a_cancel_gone_unknown_is_kept_while_its_orders_end_is_held_and_its_acceptance_is_ours() {
    // The venue accepts the cancel: the order's end was ours, nothing is noted, and the request
    // is dropped once its notes are forgotten.
    let (mut rig, a, cancel) = held_on_an_unknown_cancel();
    answer_item(&mut rig.link, 14, cancel, 0, a, accepted());
    assert!(answered_as_ours(&rig.link, cancel));
    assert!(not_ours(&rig.link).is_empty());
    assert!(!rig.link.cancels_not_ours_seen());
    assert_eq!(problems(&rig.link), Vec::<String>::new());
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(!rig.link.cancels_not_ours_seen());
}

impl Rig {
    /// Our order `a` placed, accepted and open as venue order `v-a`, its market killed, and a
    /// cancel-all of the market sent and answered `outcome` by its one item, naming no order.
    fn cancel_all_answered(&mut self, outcome: SubmitOutcome) -> (ClientOrderId, RpcId) {
        let (a, place) = self.place();
        accepted_and_open(&mut self.link, place, a, "v-a", 10);
        self.link.reg().kill(INST_A);
        let built = self.link.reg().cancel_everything(INST_A, &order_caps());
        let CancelEverything::CancelAll { command, .. } = built else {
            panic!("no cancel-all: {built:?}");
        };
        let auth = self.link.reg().authorize(ACCT, command).unwrap();
        let all = self.link.submit(&self.orders, auth).unwrap();
        self.link.on_submitted(sent(all));
        self.answer_all(12, all, outcome);
        (a, all)
    }

    /// Cancel-all `all` answered `outcome` by its one item, naming no order, as ingest sequence
    /// `seq`.
    fn answer_all(&mut self, seq: u64, all: RpcId, outcome: SubmitOutcome) {
        let item = ItemRef {
            idx: 0,
            cid: None,
            vid: None,
        };
        let body = ExecEvent::Outcome {
            rpc: all,
            item: Some(item),
            outcome,
        };
        self.link.on_exec(env(seq, 2_000 + seq, body));
    }
}

#[test]
fn a_cancel_all_gone_unknown_is_kept_while_an_order_it_covers_lives_or_its_end_is_held() {
    // Its first answer is all fbc-oms reads, but the coverage waits on the cancel-all while an
    // order it covers lives unanswered: a user cancel ending that order is held on it, and its
    // refusal, when it comes, makes that end a cancel not ours.
    let mut rig = Rig::started(order_caps());
    let (a, all) = rig.cancel_all_answered(SubmitOutcome::Unknown);
    forget_twice(&mut rig.link);
    assert_eq!(
        rig.link.requests_held(),
        1,
        "its order lives, covered unanswered"
    );
    assert!(rig.link.is_cancel_on(all, INST_A));
    rig.link.on_exec(env(13, 2_013, user_cancelled(a, "v-a")));
    assert!(not_ours(&rig.link).is_empty(), "held on the cancel-all");
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 1, "its order's end held on it");

    rig.answer_all(14, all, SubmitOutcome::Rejected(reject("refused")));
    assert!(answered_as_ours(&rig.link, all));
    assert_eq!(not_ours(&rig.link), [a]);
    assert!(rig.link.cancels_not_ours_seen());
    assert_eq!(problems(&rig.link), Vec::<String>::new());
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
}

#[test]
fn a_cancel_all_gone_unknown_is_dropped_once_the_orders_it_covers_end_otherwise() {
    // The order it covers ends by the venue's own cancel, which nothing is held on: the
    // cancel-all is dropped once its notes are forgotten, and nothing is noted.
    let mut rig = Rig::started(order_caps());
    let (a, all) = rig.cancel_all_answered(SubmitOutcome::Unknown);
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 1);
    rig.link.on_exec(env(13, 2_013, venue_cancelled(a, "v-a")));
    assert!(rig.link.registry().get(a).unwrap().state().is_terminal());
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(!rig.link.is_cancel_on(all, INST_A));
    assert!(!rig.link.cancels_not_ours_seen());
}

#[test]
fn a_cancel_whose_held_order_end_is_never_answered_is_kept_until_the_consumer_releases_it() {
    // The wait has no bound: no forget and no epoch end drops the cancel while the order's end
    // is held on it. Released, its later refusal is the session's own and notes nothing.
    let (mut rig, a, cancel) = held_on_an_unknown_cancel();
    rig.link.on_epoch_end(KEY);
    for _ in 0..5 {
        forget_twice(&mut rig.link);
    }
    assert_eq!(rig.link.requests_held(), 1, "its order's end held on it");
    assert!(rig.link.is_cancel_on(cancel, INST_A));

    rig.link.release(cancel);
    assert_eq!(rig.link.requests_held(), 0);
    let refused = SubmitOutcome::Rejected(reject("no such order"));
    answer_item(&mut rig.link, 14, cancel, 0, a, refused);
    assert!(!answered_as_ours(&rig.link, cancel));
    assert!(not_ours(&rig.link).is_empty(), "the held end left unjudged");
    assert!(!rig.link.cancels_not_ours_seen());
}

#[test]
fn a_cancel_answered_before_it_is_reported_sent_is_kept_until_it_is() {
    // No session answers a request before reporting it sent, but one so answered for good is
    // kept until the report, which then finds the request it covers its order for.
    let mut rig = Rig::started(order_caps());
    let (a, place) = rig.place();
    accepted_and_open(&mut rig.link, place, a, "v-a", 10);
    let cancel = rig.cancel(a);
    answer_item(&mut rig.link, 12, cancel, 0, a, accepted());
    assert!(answered_as_ours(&rig.link, cancel));
    forget_twice(&mut rig.link);
    forget_twice(&mut rig.link);
    assert_eq!(
        rig.link.requests_held(),
        1,
        "the place dropped, the cancel not yet reported"
    );
    assert_eq!(rig.link.cids_of(cancel), [a]);

    rig.link.on_submitted(sent(cancel));
    rig.link.on_exec(env(13, 2_013, venue_cancelled(a, "v-a")));
    forget_twice(&mut rig.link);
    assert_eq!(rig.link.requests_held(), 0);
    assert!(!rig.link.is_cancel_on(cancel, INST_A));
}
