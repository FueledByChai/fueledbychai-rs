//! FBC-8mv's done line: the offline rehearsal (decisions 0005, 0009, 0012, 0013). fbc-oms (the
//! planner, the pre-trade path and its authorizations, market states, leases in a temporary
//! directory), fbc-runtime's order-entry session and this crate's order-entry codec, built by
//! `ParadexFactory` from synthetic credentials, run against fbc-conformance's stub server
//! standing in for Paradex (`rehearsal/venue.rs`), wired as a consumer wires them
//! (`rehearsal/glue.rs`), with everything the stack logs captured at TRACE
//! (`rehearsal/capture.rs`) and everything the session sends and receives journaled (FBC-2pr's
//! journal, credential spans kept only as keyed hashes, decision 0006).
//!
//! **The seed.** Paradex's snapshot source stays `Untrustworthy` (decision 0054; the owner's
//! answer C to RB-olg-3), so a resync seeds no position and Start stays refused after it. The
//! rehearsal runs as the owner-assisted testnet run will: the registry is built for a declared
//! `TestnetRun::owner_assisted()` (decision 0067) and the market is seeded by hand from the
//! position the first resync read over REST, once that resync has ended. The codec's caps are
//! Paradex's own, unchanged.
//!
//! The values are the owner's first test values on BTC-USD-PERP at about 60000 on its 0.00001
//! size step (a lot is about $0.60): $11 resting per side (18 lots) and $50 inventory (83 lots).

#[path = "rehearsal/capture.rs"]
mod capture;
mod common;
#[path = "rehearsal/glue.rs"]
mod glue;
mod md;
#[path = "rehearsal/venue.rs"]
mod venue;

use std::cell::RefCell;
use std::future::Future;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::Vectors;
use fbc_core::{
    AccountKey, Bps, Channel, CidMint, ClientOrderId, ConnState, InstrumentId, Lots, MarketLease,
    Namespace, NamespaceLease, NewOrder, NonceBlock, NonceSource, OrderCaps, OrderKind, Secret,
    Secrets, Side, SignedLots, SizeStep, SpecTable, SubmitOutcome, Ticks, Tif, VenueConfig,
    VenueFactory, VenueSymbol, encode_cid,
};
use fbc_journal::{
    JournalWriter, NonceSourceId, RedactionKey, SinkConfig, WriterThread, journal_queue,
};
use fbc_oms::{
    AmendRefusal, ArmRefusal, CancelChoice, CancelEverything, CapRefusal, DesiredBook,
    DesiredQuote, EntryState, ExecutionPlanner, ExitKind, ExitRefusal, FillLedger, LadderConfig,
    LeaseKeys, Leases, LedgerConfig, MarketCapsConfig, OmsError, OrdState, PlanRefusal,
    PlannerConfig, PositionCheck, PreTradeCaps, Registry, Stage, StateRefusal, TestnetRun,
};
use fbc_runtime::{
    Connector, ExecControl, ExecOrders, ExecSession, ExecSessionConfig, IngestClock, Journal,
    ProxyConfig, RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::auth::{
    ACCOUNT_ADDRESS, CHAIN_ID, REFRESH, REST_URL, SIGNATURE_LIFETIME, SIGNING_KEY, TIMEOUT,
};
use fbc_venue_paradex::exec::exec_caps;
use fbc_venue_paradex::factory::{EXEC_MODE, EXEC_URL, RPC_TIMEOUT};
use glue::{Glue, GlueHandler, Note, wall_now};
use rust_decimal::Decimal;
use serde_json::Value;
use starknet_crypto::Felt;
use venue::{Book, Fate, MARKET, Order, Shared, Stub, TOKEN, lock};

const INST: InstrumentId = md::BTC;
const ACCT: AccountKey = AccountKey::new(1);
const NS: Namespace = Namespace::new(1);
/// The lease files' names: the venue's id and a label for the account, never its address.
const VENUE: &str = "PARADEX";
const ACCOUNT_LABEL: &str = "rehearsal";
/// The owner's $11 resting per side and $50 inventory, in lots of 0.00001 at about 60000.
const RESTING_CAP: i64 = 18;
const INVENTORY_CAP: i64 = 83;
/// What each test logs first, to show the capture reads its thread.
const LIVE: &str = "the capture is live on this thread";
/// How long any one step may take before the test fails.
const STEP: Duration = Duration::from_secs(10);

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// BTC-USD-PERP on its 0.1 tick and 0.00001 size step.
fn specs() -> SpecTable {
    let mut spec = md::spec(INST, MARKET);
    spec.size_step = SizeStep::new(Decimal::from_str("0.00001").unwrap()).unwrap();
    let mut table = SpecTable::new();
    table.insert(spec);
    table
}

fn symbol() -> VenueSymbol {
    specs().get(INST).unwrap().venue_symbol.clone()
}

/// The synthetic account and key of `fixtures/paradex/signing`, from the vectors' header.
fn synthetic() -> (String, String) {
    let vectors = Vectors::read();
    (
        vectors.header["account"].clone(),
        vectors.header["key"].clone(),
    )
}

fn secrets() -> Secrets {
    let (account, key) = synthetic();
    let mut creds = Secrets::new();
    creds.insert(ACCOUNT_ADDRESS, Secret::new(account));
    creds.insert(SIGNING_KEY, Secret::new(key));
    creds
}

/// Paradex's configuration for order entry on the stub.
fn config(stub: &Stub) -> VenueConfig {
    let chain = Vectors::read().header["chain_id"].clone();
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (EXEC_URL, stub.ws_url().as_str()),
        (EXEC_MODE, "orders"),
        (RPC_TIMEOUT, "5000ms"),
        (REST_URL, stub.rest_url().as_str()),
        (CHAIN_ID, chain.as_str()),
        (SIGNATURE_LIFETIME, "600s"),
        (REFRESH, "60s"),
        (TIMEOUT, "5000ms"),
    ] {
        cfg.insert(key, value);
    }
    cfg
}

/// A directory of the test's own under the system's temporary directory, removed with
/// everything in it when dropped, the test failing or not (Reviewer B RB-8mv-8 on PR #119).
struct TempDir(PathBuf);

impl std::ops::Deref for TempDir {
    type Target = std::path::Path;

    fn deref(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A lease (or journal) directory of the test's own.
fn lease_dir() -> TempDir {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-paradex-rehearsal-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

/// Nonces counted up from 1 (Paradex asks for none).
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

fn order_caps() -> OrderCaps {
    exec_caps().order
}

/// A registry declared an owner-assisted testnet run (decision 0067) under the owner's caps,
/// its lease names given.
fn registry() -> Registry {
    let limits = MarketCapsConfig {
        inventory: Some(lots(INVENTORY_CAP)),
        resting: Some(lots(RESTING_CAP)),
    };
    let caps = PreTradeCaps::new().with_market(INST, limits).unwrap();
    let keys = LeaseKeys::new(VENUE, ACCOUNT_LABEL, &order_caps()).with_market(INST, symbol());
    Registry::with_caps(caps)
        .for_testnet_run(TestnetRun::owner_assisted())
        .with_lease_keys(keys)
}

/// The market's lease (Paradex signs no nonce, so no account lease is needed).
fn market_lease(dir: &std::path::Path) -> Leases {
    Leases::market(MarketLease::acquire(dir, VENUE, ACCOUNT_LABEL, &symbol()).unwrap())
}

/// A wired session: the glue (registry and ledger), the session, its orders and its control,
/// and its journal: the writer and the directory it writes.
struct Wired {
    glue: Rc<RefCell<Glue>>,
    session: ExecSession<GlueHandler>,
    orders: ExecOrders,
    control: ExecControl,
    writer: WriterThread,
    journal: TempDir,
}

/// The shard whose journal the session writes.
const SHARD: u16 = 0;

/// Every byte the journal under `dir` holds as written, in path order, a closed segment
/// decompressed.
fn journal_bytes(dir: &std::path::Path) -> Vec<u8> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        if path.is_dir() {
            out.extend(journal_bytes(&path));
        } else if path.extension().is_some_and(|e| e == "zst") {
            out.extend(zstd::decode_all(std::fs::File::open(&path).unwrap()).unwrap());
        } else {
            out.extend(std::fs::read(&path).unwrap());
        }
    }
    out
}

/// Closes `writer` and reads back the journal under `dir` as text (every credential the
/// rehearsal knows is ASCII, so a lossy read keeps it if it is there): it recorded the auth
/// frame and the order frames, and no token, login signature or key.
fn journal_clean(writer: WriterThread, dir: &std::path::Path, signatures: &[String]) {
    writer.close().unwrap();
    let journal = String::from_utf8_lossy(&journal_bytes(dir)).into_owned();
    assert!(
        journal.contains(r#""method":"auth""#),
        "the auth frame is journaled"
    );
    assert!(journal.contains("order.cancel_on_disconnect"));
    assert!(journal.contains("order.create"));
    secrets_absent(&journal, signatures, "the journal");
}

fn wire(stub: &Stub, reg: Registry) -> Wired {
    let ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3_600),
            max_entries: 10_000,
        },
        wall_now(),
    )
    .unwrap();
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    let glue = Rc::new(RefCell::new(Glue::new(reg, ledger, order_caps(), ladder)));
    let venue: &'static ParadexFactory = &ParadexFactory;
    let cfg = config(stub);
    let caps = venue.caps(&cfg).unwrap();
    let session_cfg = ExecSessionConfig {
        venue,
        cfg,
        creds: secrets(),
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: NS,
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            20,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap(),
        clock: IngestClock::new(),
        nonces: Box::new(Counting(1)),
        nonce_source: NonceSourceId(1),
        conn: 0,
        limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(10).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(10)).unwrap(),
        http_max_body: 1 << 20,
    };
    let (mut session, control) =
        ExecSession::new(session_cfg, GlueHandler(Rc::clone(&glue))).unwrap();
    // The session journals everything it sends and receives (FBC-2pr), credential spans as
    // keyed hashes.
    let journal = lease_dir();
    let key = Arc::new(RedactionKey::new(&[9; 32]).unwrap());
    let sink_config = SinkConfig {
        budget_bytes: 4 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(sink_config, Arc::clone(&key)).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&*journal, SHARD, key).unwrap())
        .unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(sink))));
    let orders = session.orders();
    Wired {
        glue,
        session,
        orders,
        control,
        writer,
        journal,
    }
}

/// Runs `session` beside `driver` until the driver is done; the session ending first fails the
/// test.
async fn drive<T>(session: &mut ExecSession<GlueHandler>, driver: impl Future<Output = T>) -> T {
    let running = session.run();
    tokio::pin!(running);
    tokio::pin!(driver);
    tokio::select! {
        ended = &mut running => panic!("the session ended before the rehearsal: {ended:?}"),
        out = &mut driver => out,
    }
}

/// Waits, letting the session run, until `done` holds of the glue.
async fn until(glue: &Rc<RefCell<Glue>>, what: &str, done: impl Fn(&Glue) -> bool) {
    let deadline = tokio::time::Instant::now() + STEP;
    while !done(&glue.borrow()) {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "timed out waiting for {what}; notes: {:#?}",
                glue.borrow().notes
            );
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn resyncs(g: &Glue) -> usize {
    g.notes
        .iter()
        .filter(|n| matches!(n, Note::Resynced { .. }))
        .count()
}

fn state_of(g: &Glue, cid: ClientOrderId) -> Option<OrdState> {
    g.reg.get(cid).map(|r| r.state())
}

fn is_open(g: &Glue, cid: ClientOrderId) -> bool {
    matches!(
        state_of(g, cid),
        Some(OrdState::Open | OrdState::PartiallyFilled)
    )
}

fn is_terminal(g: &Glue, cid: ClientOrderId) -> bool {
    state_of(g, cid).is_some_and(|s| s.is_terminal())
}

fn problems(g: &Glue) -> Vec<String> {
    g.notes
        .iter()
        .filter_map(|n| match n {
            Note::Problem(p) => Some(p.clone()),
            _ => None,
        })
        .collect()
}

/// A post-only buy of `qty` lots at `px` ticks, minted now.
fn buy(mint: &mut CidMint, px: i64, qty: i64) -> NewOrder {
    NewOrder {
        cid: mint.mint().unwrap(),
        inst: INST,
        side: Side::Buy,
        kind: OrderKind::Limit { px: Ticks(px) },
        qty: lots(qty),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// A post-only quote of `qty` lots at `px` ticks, reduce-only and classified reducing as
/// `reducing` says.
fn quote(px: i64, qty: i64, reducing: bool) -> DesiredQuote {
    DesiredQuote {
        px: Ticks(px),
        qty: lots(qty),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: reducing,
        reducing,
    }
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(PlannerConfig::new(Bps(1.0), lots(1), Duration::ZERO).unwrap())
}

/// The order-affecting methods among `methods`.
fn order_methods(methods: &[String]) -> Vec<String> {
    methods
        .iter()
        .filter(|m| m.starts_with("order.") && *m != "order.cancel_on_disconnect")
        .cloned()
        .collect()
}

/// What a fresh connection writes before any order method, in some order: the auth frame, the
/// four private subscriptions and the cancel-on-disconnect arm.
fn opening(methods: &[String]) {
    let mut sorted = methods.to_vec();
    sorted.sort();
    assert_eq!(
        sorted,
        [
            "auth",
            "order.cancel_on_disconnect",
            "subscribe",
            "subscribe",
            "subscribe",
            "subscribe"
        ],
        "{methods:?}"
    );
}

/// No credential, session token or login signature in `text`: the synthetic account and key
/// (as hex, with or without `0x`, any case), the stub's token, and each part of each login
/// signature the stub read.
fn secrets_absent(text: &str, signatures: &[String], what: &str) {
    let lower = text.to_ascii_lowercase();
    for needle in credential_needles(signatures) {
        assert!(
            !lower.contains(&needle),
            "{what} shows a credential, the token or a login signature"
        );
    }
}

/// What [`secrets_absent`] looks for, lowercase: the account, the key, the session token and the
/// login signatures' numbers, each as text and as the hex of its bytes, as tungstenite dumps a
/// payload. A number is also looked for as starknet's `Felt` prints it: in hex without leading
/// zeros (which `{:x}`, `{:#x}` and any zero-padded form contain) and in decimal.
fn credential_needles(signatures: &[String]) -> Vec<String> {
    let (account, key) = synthetic();
    let mut numbers = vec![account, key];
    for sig in signatures {
        assert!(!sig.is_empty(), "the stub read no login signature");
        // The signature's numbers, not its punctuation or a short fragment.
        let parts = sig.split(|c: char| !c.is_ascii_alphanumeric());
        numbers.extend(
            parts
                .filter(|p| p.trim_start_matches("0x").len() >= 16)
                .map(str::to_owned),
        );
    }
    let mut secrets = vec![TOKEN.to_owned()];
    for number in numbers {
        let digits = number.trim_start_matches("0x");
        let felt = if number.starts_with("0x") || !digits.bytes().all(|b| b.is_ascii_digit()) {
            Felt::from_hex(&format!("0x{digits}"))
        } else {
            Felt::from_dec_str(digits)
        };
        let felt = felt.unwrap_or_else(|_| panic!("{number} is not a field element"));
        secrets.push(digits.to_owned());
        secrets.push(
            format!("{felt:x}")
                .trim_start_matches("0x")
                .trim_start_matches('0')
                .to_owned(),
        );
        secrets.push(format!("{felt}"));
    }
    let mut needles = Vec::new();
    for secret in secrets {
        let hex: String = secret.bytes().map(|b| format!("{b:02x}")).collect();
        needles.push(secret.to_ascii_lowercase());
        needles.push(hex);
    }
    needles
}

/// Every record this thread logged, one per line.
fn trace_output() -> String {
    capture::of_this_thread()
        .iter()
        .map(|l| format!("{} {} {}", l.level, l.target, l.text))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A field element is how the account, the key and a signature's numbers would most likely reach
/// a log: printed by starknet's `Felt`, in hex without the leading zeros the fixture keeps, or in
/// decimal (Reviewer B RB-8mv-2 on PR #119). [`secrets_absent`] has to see every such form.
#[test]
fn the_secret_search_sees_a_field_element_in_every_form_it_prints() {
    let (account, key) = synthetic();
    let needles = credential_needles(&[]);
    for value in [account, key] {
        let felt = Felt::from_hex(&value).unwrap();
        for printed in [
            format!("{felt}"),
            format!("{felt:?}"),
            format!("{felt:x}"),
            format!("{felt:#x}"),
            format!("{felt:#064x}"),
        ] {
            let printed = printed.to_ascii_lowercase();
            let hex: String = printed.bytes().map(|b| format!("{b:02x}")).collect();
            for shown in [&printed, &hex] {
                assert!(
                    needles.iter().any(|n| shown.contains(n.as_str())),
                    "the search misses {value} printed as {printed}"
                );
            }
        }
    }
}

/// From a fresh start: no place or amend frame until Start; Start refused until the first
/// resync has ended (and, the snapshot source untrustworthy, until the owner's hand seed); the
/// arm accepted before the first order frame; an earlier run's order cancelled on the disarmed
/// market before Start; a placed order acknowledged and filled moving the inventory once, its
/// fill repeated; a place over the resting cap, a place or amend while Killed and any ordinary
/// order once the switch is lifted never written while a cancel is; a reconnect while an order
/// rests on a Quoting market re-authenticating, re-arming and resyncing with nothing re-placed; a
/// second holder of the market lease unable to arm; and no token, login signature or key in the
/// TRACE output or the journal.
#[tokio::test]
async fn from_a_fresh_start_every_safety_rule_holds_against_the_stub() {
    capture::install();
    log::debug!(target: "rehearsal", "{LIVE}");
    let dir = lease_dir();
    let book: Shared = Arc::new(Mutex::new(Book::default()));
    // Connection 0: the opening six, the orphan's cancel, A's place, B's place, B's cancel and
    // C's place; then the stub closes it, C resting on a Quoting market. Connection 1: the
    // opening six and the kill switch's cancel of C.
    let stub = Stub::start(&book, &[11, 7]);
    let mut mint = CidMint::new(
        NamespaceLease::acquire(&dir, ACCT, NS).unwrap(),
        0,
        0,
        wall_now(),
    );
    // An order of ours an earlier run left resting.
    let orphan = mint.mint().unwrap();
    let orphan_wire = encode_cid(&order_caps().client_id, orphan).unwrap();
    lock(&book).orders.insert(
        "1759500000000000901".to_owned(),
        Order {
            vid: "1759500000000000901".to_owned(),
            cid: orphan_wire.as_str().to_owned(),
            side: "BUY",
            price: "59000".to_owned(),
            size: "0.00005".to_owned(),
            reduce_only: false,
            open: true,
        },
    );

    let mut reg = registry();
    // Before anything is read from the venue, the position is unknown: Start is refused, and
    // nothing can be built to place.
    assert_eq!(
        reg.start(INST, market_lease(&dir)),
        Err(ArmRefusal::PositionUnknown(INST))
    );
    assert_eq!(
        reg.place(buy(&mut mint, 599_900, 1)).err(),
        Some(OmsError::State(StateRefusal::CancelOnly(INST)))
    );
    let Wired {
        glue,
        mut session,
        orders,
        control,
        writer,
        journal,
    } = wire(&stub, reg);
    let mut planner = planner();
    let caps = order_caps();

    let g = Rc::clone(&glue);
    let kept = Arc::clone(&book);
    let script = async move {
        let glue = g;
        // The session takes no place or amend until its epoch's arm is accepted and its
        // resync has ended.
        assert!(!orders.may_place());
        // The first epoch: login, auth, subscriptions, the arm and the resync.
        until(&glue, "the first resync", |g| resyncs(g) == 1).await;
        assert!(orders.may_place());
        {
            let mut g = glue.borrow_mut();
            let (report, snapshot) =
                match g.notes.iter().find(|n| matches!(n, Note::Resynced { .. })) {
                    Some(Note::Resynced { report, snapshot }) => (report.clone(), snapshot.clone()),
                    _ => unreachable!(),
                };
            // The snapshot source is untrustworthy: the resync seeded nothing, so Start is
            // still refused, and the planner builds nothing.
            assert!(report.untrustworthy, "{report:?}");
            assert!(report.seeded.is_empty(), "{report:?}");
            assert!(!g.reg.position_known(INST));
            assert_eq!(
                g.reg.start(INST, Leases::none()),
                Err(ArmRefusal::PositionUnknown(INST))
            );
            let (now, _) = g.now();
            let want = DesiredBook::new(INST).with(Side::Buy, 0, quote(599_900, 10, false));
            let plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.commands.is_empty(), "{plan:?}");
            // The earlier run's order is ours and resting: it is cancelled before Start, on the
            // disarmed market in Cancel-only (0012).
            assert_eq!(snapshot.orders.len(), 1);
            assert!(!g.reg.entry(INST).armed());
            let permit = g.reg.cancellable(orphan).unwrap();
            let CancelChoice::Send(cmd) = permit.cancel(&caps) else {
                panic!("the orphan's cancel is sent");
            };
            let auth = g.reg.authorize(ACCT, cmd).unwrap();
            g.submit(&orders, auth);
        }
        until(&glue, "the orphan's cancel", |g| is_terminal(g, orphan)).await;
        let before_start = stub.methods(0);
        assert_eq!(
            order_methods(&before_start),
            ["order.cancel"],
            "{before_start:?}"
        );

        // The owner's hand seed, from the position the resync read (flat), then Start.
        {
            let mut g = glue.borrow_mut();
            let flat = match g.notes.iter().find(|n| matches!(n, Note::Resynced { .. })) {
                Some(Note::Resynced { snapshot, .. }) => snapshot
                    .positions
                    .iter()
                    .find(|(i, _)| *i == INST)
                    .map_or(SignedLots(0), |(_, q)| *q),
                _ => unreachable!(),
            };
            g.reg.seed_position(INST, flat).unwrap();
            let entry = g.reg.start(INST, market_lease(&dir)).unwrap();
            assert_eq!(entry.state(), EntryState::Quoting);
        }

        // A, planned at level 0 and filled whole: its fill comes twice, the inventory moves
        // once.
        lock(&book).fates.push_back(Fate::Fill);
        let a = {
            let mut g = glue.borrow_mut();
            let (now, _) = g.now();
            let want = DesiredBook::new(INST).with(Side::Buy, 0, quote(599_900, 10, false));
            let mut plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.refused.is_empty(), "{plan:?}");
            assert_eq!(plan.commands.len(), 1, "{plan:?}");
            let planned = plan.commands.remove(0);
            assert_eq!(planned.stage, Stage::Add);
            g.submit(&orders, planned.auth);
            planned.cid
        };
        until(&glue, "A filled", |g| is_terminal(g, a)).await;
        until(&glue, "both fills seen", |g| {
            g.notes
                .iter()
                .filter(|n| matches!(n, Note::Fill { .. }))
                .count()
                == 2
        })
        .await;
        {
            let g = glue.borrow();
            let fills: Vec<_> = g
                .notes
                .iter()
                .filter_map(|n| match n {
                    Note::Fill { applied, what } => Some((*applied, what.clone())),
                    _ => None,
                })
                .collect();
            assert!(fills[0].0, "{fills:?}");
            assert!(!fills[1].0 && fills[1].1 == "Duplicate", "{fills:?}");
            assert_eq!(g.reg.inventory(INST), SignedLots(10));
            assert_eq!(
                state_of(&g, a),
                Some(OrdState::Terminal(fbc_oms::TerminalKind::Filled))
            );
        }

        // B rests; a place that would take the side's resting orders past the cap is refused,
        // directly and through the planner, and only B's cancel is written.
        lock(&book).fates.push_back(Fate::Rest);
        let b = {
            let mut g = glue.borrow_mut();
            let cmd = g.reg.place(buy(&mut mint, 599_800, 5)).unwrap();
            let auth = g.reg.authorize(ACCT, cmd).unwrap();
            let order = auth.command().clone();
            g.submit(&orders, auth);
            match order {
                fbc_core::VenueCommand::Place(o) => o.cid,
                other => panic!("{other:?}"),
            }
        };
        until(&glue, "B open", |g| is_open(g, b)).await;
        {
            let mut g = glue.borrow_mut();
            let over = g.reg.place(buy(&mut mint, 599_700, RESTING_CAP - 5 + 1));
            assert!(
                matches!(over, Err(OmsError::Capped(CapRefusal::RestingCap { .. }))),
                "{over:?}"
            );
            let (now, _) = g.now();
            let want = DesiredBook::new(INST).with(
                Side::Buy,
                0,
                quote(599_700, RESTING_CAP - 5 + 1, false),
            );
            let plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.commands.is_empty(), "{plan:?}");
            assert!(
                matches!(
                    plan.refused.as_slice(),
                    [r] if matches!(r.why, PlanRefusal::Place(OmsError::Capped(CapRefusal::RestingCap { .. })))
                ),
                "{plan:?}"
            );
            let CancelChoice::Send(cmd) = g.reg.cancellable(b).unwrap().cancel(&caps) else {
                panic!("B's cancel is sent");
            };
            let auth = g.reg.authorize(ACCT, cmd).unwrap();
            g.submit(&orders, auth);
        }
        until(&glue, "B cancelled", |g| is_terminal(g, b)).await;

        // C rests, and the stub closes the connection once C's place is answered.
        lock(&book).fates.push_back(Fate::Rest);
        let c = {
            let mut g = glue.borrow_mut();
            let cmd = g.reg.place(buy(&mut mint, 599_800, 5)).unwrap();
            let auth = g.reg.authorize(ACCT, cmd).unwrap();
            let cid = match auth.command() {
                fbc_core::VenueCommand::Place(o) => o.cid,
                other => panic!("{other:?}"),
            };
            g.submit(&orders, auth);
            cid
        };
        until(&glue, "C open", |g| is_open(g, c)).await;
        // On the wire: the opening six, then the orphan's cancel, A, B, B's cancel and C; the
        // arm accepted before the first order frame.
        let conn0 = stub.methods(0);
        opening(&conn0[..6]);
        assert_eq!(
            order_methods(&conn0),
            [
                "order.cancel",
                "order.create",
                "order.create",
                "order.cancel",
                "order.create"
            ],
            "{conn0:?}"
        );
        {
            let g = glue.borrow();
            let armed = g.notes.iter().position(|n| {
                matches!(
                    n,
                    Note::Outcome {
                        ours: false,
                        outcome: SubmitOutcome::Accepted { .. },
                        ..
                    }
                )
            });
            let first_order = g.notes.iter().position(
                |n| matches!(n, Note::Submitted { rpc, sent: Ok(()) } if g.is_ours(*rpc)),
            );
            assert!(armed.unwrap() < first_order.unwrap(), "{:#?}", g.notes);
        }

        // The stub closes the connection with C resting and the market Quoting (Reviewer B
        // RB-8mv-5 on PR #119): the session reconnects, authenticates again, re-arms and
        // resyncs, and writes nothing else, neither C again nor anything for it. The stub does
        // not model cancel-on-disconnect, so the resync finds C still resting.
        until(&glue, "the second resync", |g| resyncs(g) == 2).await;
        // A while for anything the reconnect might still write.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let conn1 = stub.methods(1);
        opening(&conn1);
        {
            let g = glue.borrow();
            let Some(Note::Resynced { report, snapshot }) = g
                .notes
                .iter()
                .rev()
                .find(|n| matches!(n, Note::Resynced { .. }))
            else {
                unreachable!()
            };
            // The venue's position agrees with the inventory, which moved once, and C is the one
            // order the venue holds, still open in the registry.
            assert_eq!(g.reg.inventory(INST), SignedLots(10));
            assert_eq!(
                report.checks,
                [PositionCheck::Agrees {
                    inst: INST,
                    position: SignedLots(10)
                }],
                "{report:?}"
            );
            assert_eq!(snapshot.orders.len(), 1, "{snapshot:?}");
            assert!(is_open(&g, c));
            assert_eq!(g.reg.entry(INST).state(), EntryState::Quoting);
            assert_eq!(
                g.notes
                    .iter()
                    .filter(|n| matches!(n, Note::Conn(ConnState::Authenticated)))
                    .count(),
                2
            );
        }
        // One login: the reconnect authenticates with the token it already holds.
        assert_eq!(lock(&book).login_signatures.len(), 1);

        // The kill switch: no place, no amend, no planned order; its cancel everything is
        // written. Lifted: Cancel-only, so still no ordinary order.
        {
            let mut g = glue.borrow_mut();
            g.reg.kill(INST);
            assert_eq!(
                g.reg.place(buy(&mut mint, 599_800, 1)).err(),
                Some(OmsError::State(StateRefusal::Killed(INST)))
            );
            let amend = g
                .reg
                .live(c)
                .map(|live| live.amend(&caps, Ticks(599_700), lots(5), false).err());
            assert_eq!(
                amend,
                Ok(Some(AmendRefusal::State(StateRefusal::Killed(INST))))
            );
            let (now, _) = g.now();
            let want = DesiredBook::new(INST).with(Side::Buy, 1, quote(599_600, 1, false));
            let plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.commands.is_empty(), "{plan:?}");
            let CancelEverything::Explicit { plan, .. } = g.reg.cancel_everything(INST, &caps)
            else {
                panic!("no cancel-all under an untrustworthy snapshot (0005's I7)");
            };
            for cmd in plan.commands {
                let auth = g.reg.authorize(ACCT, cmd).unwrap();
                g.submit(&orders, auth);
            }
        }
        until(&glue, "C cancelled", |g| is_terminal(g, c)).await;
        {
            let mut g = glue.borrow_mut();
            let entry = g.reg.lift_kill(INST);
            assert_eq!(entry.state(), EntryState::CancelOnly);
            assert!(entry.armed());
            assert_eq!(
                g.reg.place(buy(&mut mint, 599_800, 1)).err(),
                Some(OmsError::State(StateRefusal::CancelOnly(INST)))
            );
            let (now, _) = g.now();
            let want = DesiredBook::new(INST).with(Side::Buy, 1, quote(599_600, 1, false));
            let plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.commands.is_empty(), "{plan:?}");
        }
        // On the second connection: the opening six, then only the kill switch's cancel of C.
        let conn1 = stub.methods(1);
        opening(&conn1[..6]);
        assert_eq!(order_methods(&conn1), ["order.cancel_batch"], "{conn1:?}");
        {
            let g = glue.borrow();
            assert!(problems(&g).is_empty(), "{:?}", problems(&g));
        }

        // A second holder of the market lease cannot arm: the lease is held, and a registry
        // without it is refused.
        assert!(MarketLease::acquire(&dir, VENUE, ACCOUNT_LABEL, &symbol()).is_err());
        let mut second = registry();
        second.seed_position(INST, SignedLots(10)).unwrap();
        assert_eq!(
            second.start(INST, Leases::none()),
            Err(ArmRefusal::NoMarketLease(INST))
        );
        assert!(!second.entry(INST).armed());

        drop(control);
        stub
    };
    let stub = drive(&mut session, script).await;
    stub.finished().await.unwrap();

    // No token, login signature or key in what the session's thread logged at TRACE, nor in the
    // frames it wrote (the session token goes in the auth frame, which the stub read; only
    // there).
    let signatures = lock(&kept).login_signatures.clone();
    let traced = trace_output();
    assert!(
        traced.contains(LIVE),
        "the capture missed this thread's records"
    );
    secrets_absent(&traced, &signatures, "the TRACE output");
    journal_clean(writer, &journal, &signatures);
    let auth_frames: Vec<String> = stub
        .texts()
        .into_iter()
        .filter(|t| t.contains(r#""method":"auth""#))
        .collect();
    assert_eq!(auth_frames.len(), 2);
    let others: String = stub
        .texts()
        .into_iter()
        .filter(|t| !t.contains(r#""method":"auth""#))
        .collect();
    secrets_absent(&others, &signatures, "a frame other than the auth frame");
}

/// After a restart: the venue shows a long position and nothing resting. Flatten is refused
/// until the first resync has ended and the owner's seed; then it arms the market straight into
/// Exit, and only reduce-only sells that never take the position past zero are written, even
/// when the desired book asks for more.
#[tokio::test]
async fn after_a_restart_flatten_writes_only_reduce_only_exits_that_never_cross_zero() {
    capture::install();
    log::debug!(target: "rehearsal", "{LIVE}");
    let dir = lease_dir();
    let book: Shared = Arc::new(Mutex::new(Book::default()));
    // Long 0.0001: 10 lots, left by an earlier run.
    lock(&book).position = Decimal::from_str("0.0001").unwrap();
    // The opening six, then one exit order.
    let stub = Stub::start(&book, &[7]);
    let mut mint = CidMint::new(
        NamespaceLease::acquire(&dir, ACCT, NS).unwrap(),
        0,
        0,
        wall_now(),
    );
    let mut reg = registry();
    assert_eq!(
        reg.flatten(INST, market_lease(&dir)),
        Err(ArmRefusal::PositionUnknown(INST))
    );
    let Wired {
        glue,
        mut session,
        orders,
        control,
        writer,
        journal,
    } = wire(&stub, reg);
    let mut planner = planner();
    let caps = order_caps();

    let g = Rc::clone(&glue);
    let kept = Arc::clone(&book);
    let script = async move {
        let glue = g;
        until(&glue, "the first resync", |g| resyncs(g) == 1).await;
        let exit = {
            let mut g = glue.borrow_mut();
            let pos = match g.notes.iter().find(|n| matches!(n, Note::Resynced { .. })) {
                Some(Note::Resynced { report, snapshot }) => {
                    assert!(
                        report.untrustworthy && report.seeded.is_empty(),
                        "{report:?}"
                    );
                    snapshot
                        .positions
                        .iter()
                        .find(|(i, _)| *i == INST)
                        .unwrap()
                        .1
                }
                _ => unreachable!(),
            };
            assert_eq!(pos, SignedLots(10));
            assert_eq!(
                g.reg.flatten(INST, Leases::none()),
                Err(ArmRefusal::PositionUnknown(INST))
            );
            g.reg.seed_position(INST, pos).unwrap();
            let entry = g.reg.flatten(INST, market_lease(&dir)).unwrap();
            assert_eq!(entry.state(), EntryState::Exit(ExitKind::Flatten));

            // The quoting book: a buy that would add to the position, an ordinary sell, and a
            // reduce-only sell larger than the position. Only exits are built, none past zero.
            let (now, _) = g.now();
            let want = DesiredBook::new(INST)
                .with(Side::Buy, 0, quote(599_900, 5, false))
                .with(Side::Sell, 0, quote(600_100, 15, true))
                .with(Side::Sell, 1, quote(600_200, 5, false));
            let plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.commands.is_empty(), "{plan:?}");
            assert_eq!(plan.refused.len(), 3, "{plan:?}");
            for r in &plan.refused {
                let PlanRefusal::Place(OmsError::State(StateRefusal::Exit(why))) = &r.why else {
                    panic!("{plan:?}");
                };
                match (r.side, r.level) {
                    (Side::Buy, 0) => assert!(matches!(why, ExitRefusal::Increasing { .. })),
                    (Side::Sell, 1) => assert!(matches!(why, ExitRefusal::Ordinary { .. })),
                    // Reduce-only, but past zero (Reviewer B RB-8mv-6 on PR #119).
                    (Side::Sell, 0) => assert!(matches!(why, ExitRefusal::CrossesZero { .. })),
                    other => panic!("an unexpected refusal at {other:?}: {plan:?}"),
                }
            }

            // The reduce-only sell of the position: built, a reducing order, and written.
            lock(&book).fates.push_back(Fate::Rest);
            let want = DesiredBook::new(INST).with(Side::Sell, 0, quote(600_100, 10, true));
            let mut plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.refused.is_empty(), "{plan:?}");
            assert_eq!(plan.commands.len(), 1, "{plan:?}");
            let planned = plan.commands.remove(0);
            assert_eq!((planned.stage, planned.side), (Stage::Reducing, Side::Sell));
            g.submit(&orders, planned.auth);
            planned.cid
        };
        until(&glue, "the exit resting", |g| is_open(g, exit)).await;
        {
            // With the whole position resting to exit, one more lot would cross zero.
            let mut g = glue.borrow_mut();
            let (now, _) = g.now();
            let want = DesiredBook::new(INST)
                .with(Side::Sell, 0, quote(600_100, 10, true))
                .with(Side::Sell, 1, quote(600_200, 1, true));
            let plan = planner
                .plan(&want, &mut g.reg, &caps, ACCT, &mut mint, now)
                .unwrap();
            assert!(plan.commands.is_empty(), "{plan:?}");
            assert!(
                matches!(
                    plan.refused.as_slice(),
                    [r] if (r.side, r.level) == (Side::Sell, 1) && matches!(
                        r.why,
                        PlanRefusal::Place(OmsError::State(StateRefusal::Exit(
                            ExitRefusal::CrossesZero { .. }
                        )))
                    )
                ),
                "{plan:?}"
            );
            assert!(problems(&g).is_empty(), "{:?}", problems(&g));
        }
        let conn0 = stub.methods(0);
        opening(&conn0[..6]);
        assert_eq!(order_methods(&conn0), ["order.create"], "{conn0:?}");
        let create: Value = stub
            .requests(0)
            .into_iter()
            .find(|r| r["method"] == "order.create")
            .unwrap();
        assert_eq!(create["params"]["side"], "SELL");
        assert_eq!(
            create["params"]["flags"],
            serde_json::json!(["REDUCE_ONLY"])
        );
        assert_eq!(create["params"]["size"], "0.0001");
        drop(control);
        stub
    };
    let stub = drive(&mut session, script).await;
    stub.finished().await.unwrap();
    let signatures = lock(&kept).login_signatures.clone();
    let traced = trace_output();
    assert!(
        traced.contains(LIVE),
        "the capture missed this thread's records"
    );
    secrets_absent(&traced, &signatures, "the TRACE output");
    journal_clean(writer, &journal, &signatures);
}
