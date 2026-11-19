//! The samples' glue (`src/link.rs`) reports each order-entry connection epoch's end to the
//! account's fbc-oms registry (FBC-9svb), at the ended epoch's last envelope's `recv_mono`, so
//! the registry builds no instrument cancel-all until a trustworthy resync requested after the
//! end. The handler is driven here as an order-entry session drives it: every envelope of an
//! epoch, in ingest order, then the epoch's end, then the next epoch's envelopes. On a market
//! the link's registry holds armed, the kill switch's cancel everything is then explicit from
//! the end until the next epoch's resync. On a session of several connections, the end reported
//! is the last envelope of any of them.

// Shared by the samples that trade: this test uses a part of it.
#[allow(dead_code)]
#[path = "../src/link.rs"]
mod link;

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_core::{
    CancelScope, ConnKey, ConnState, Envelope, ExchTsKind, ExecEvent, InstrumentId, Lots,
    MarketLease, MonoNs, Stamp, StreamId, VenueCommand, WallNs, dispatch_market_data,
};
use fbc_oms::{
    CancelAllRefusal, CancelEverything, FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig,
    MarketCapsConfig, PreTradeCaps, Registry,
};
use fbc_runtime::ExecHandler;
use link::{Link, LinkHandler, Note};
use tokio::sync::Notify;

/// The glue over a fresh registry under Paradex's order caps, whose snapshot is trustworthy.
fn handler() -> LinkHandler {
    handler_over(Registry::new())
}

/// The glue over `reg` under Paradex's order caps.
fn handler_over(reg: Registry) -> LinkHandler {
    let caps = fbc_venue_paradex::exec::exec_caps().order;
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
    LinkHandler {
        link: Rc::new(RefCell::new(Link::new(reg, ledger, caps, ladder))),
        wake: Rc::new(Notify::new()),
    }
}

/// Connection 0's epoch `epoch`.
fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: 0, epoch }
}

/// `body`, the `seq`th input of the shard, received at `at` on connection 0's epoch `epoch`.
fn env(seq: u64, epoch: u32, at: u64, body: ExecEvent) -> Envelope<ExecEvent> {
    env_on(seq, key(epoch), at, body)
}

/// `body`, the `seq`th input of the shard, received at `at` on `conn`.
fn env_on(seq: u64, conn: ConnKey, at: u64, body: ExecEvent) -> Envelope<ExecEvent> {
    Envelope {
        stamp: Stamp {
            ingest_seq: seq,
            kernel_rx: None,
            recv_mono: MonoNs(at),
            recv_wall: WallNs(i64::try_from(at).unwrap()),
            conn,
        },
        exch_ts: None,
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: None,
        body,
    }
}

fn conn(state: ConnState) -> ExecEvent {
    ExecEvent::Conn {
        stream: StreamId(0),
        state,
    }
}

/// Hands `h` one epoch as a session does: connecting at `at`, open, authenticated and a resync
/// of nothing answered, one event every 10 ns from `at`, starting at ingest sequence `seq`.
/// Returns the last envelope's receive time.
fn epoch(h: &mut LinkHandler, seq: u64, epoch: u32, at: u64) -> MonoNs {
    let bodies = [
        conn(ConnState::Connecting),
        conn(ConnState::Open),
        conn(ConnState::Authenticated),
        ExecEvent::ResyncBegin {
            watermark: WallNs(i64::try_from(at).unwrap()),
        },
        ExecEvent::ResyncEnd,
    ];
    let mut last = MonoNs(0);
    for (n, body) in (0u64..).zip(bodies) {
        last = MonoNs(at + 10 * n);
        h.on_exec(env(seq + n, epoch, last.0, body));
    }
    last
}

/// The registry's latest order-entry epoch end.
fn end_of(h: &LinkHandler) -> Option<MonoNs> {
    h.link.borrow().registry().order_entry_epoch_end()
}

/// The `requested_at` of each resync the glue applied, in order.
fn requested(h: &LinkHandler) -> Vec<MonoNs> {
    h.link
        .borrow()
        .notes()
        .iter()
        .filter_map(|n| match n {
            Note::Resynced { snapshot, .. } => Some(snapshot.requested_at),
            _ => None,
        })
        .collect()
}

#[test]
fn the_glue_reports_each_order_entry_epoch_end_to_the_registry_at_the_ended_epochs_last_envelope() {
    let mut h = handler();
    assert_eq!(end_of(&h), None);

    // An epoch that ends before the session handed any envelope: the clock's origin.
    h.on_epoch_end(key(0));
    assert_eq!(end_of(&h), Some(MonoNs(0)));

    // The next epoch's events move nothing until it ends; its end is its last envelope's.
    let first = epoch(&mut h, 0, 1, 1_000);
    assert_eq!(first, MonoNs(1_040));
    assert_eq!(end_of(&h), Some(MonoNs(0)));
    h.on_epoch_end(key(1));
    assert_eq!(end_of(&h), Some(first));

    // And again for the epoch after it.
    let second = epoch(&mut h, 5, 2, 2_000);
    assert_eq!(end_of(&h), Some(first));
    h.on_epoch_end(key(2));
    assert_eq!(end_of(&h), Some(second));

    // Each epoch's resync was requested at its authentication, so the one after an end is
    // requested after it: the registry views the account's open orders again from it.
    assert_eq!(requested(&h), vec![MonoNs(1_020), MonoNs(2_020)]);
    assert!(MonoNs(2_020) > first);

    // Every end is still noted for the driver, once each.
    let ends: Vec<ConnKey> = h
        .link
        .borrow()
        .notes()
        .iter()
        .filter_map(|n| match n {
            Note::EpochEnd { key, .. } => Some(*key),
            _ => None,
        })
        .collect();
    assert_eq!(ends, vec![key(0), key(1), key(2)]);
}

/// The market the armed case holds.
const INST: InstrumentId = InstrumentId::new(1);

/// A lease directory of this test's own, removed when it is dropped.
struct LeaseDir(PathBuf);

impl LeaseDir {
    fn new() -> LeaseDir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "fbc-link-epoch-end-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        LeaseDir(dir)
    }
}

impl Drop for LeaseDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The kill switch's cancel everything for `INST` on the link's registry, under the link's
/// Paradex order caps: the refusal that kept it explicit, or `None` for an instrument
/// cancel-all.
fn refusal(h: &LinkHandler) -> Option<CancelAllRefusal> {
    let caps = fbc_venue_paradex::exec::exec_caps().order;
    match h.link.borrow_mut().reg().cancel_everything(INST, &caps) {
        CancelEverything::CancelAll { command, .. } => {
            assert_eq!(
                command.command(),
                &VenueCommand::CancelAll(CancelScope::Instrument(INST))
            );
            None
        }
        CancelEverything::Explicit { why, .. } => Some(why),
    }
}

#[test]
fn on_an_armed_market_the_glues_epoch_end_keeps_the_cancel_everything_explicit_until_a_resync() {
    // A registry named for a test account on Paradex, `INST` its one market under test caps,
    // whose nonces need no account lease.
    let caps = fbc_venue_paradex::exec::exec_caps().order;
    let paradex = fbc_venue_paradex::factory::caps();
    let symbol = dispatch_market_data(&paradex, |s| s.venue_symbol("SYN-USD-PERP")).unwrap();
    let keys = LeaseKeys::new("paradex", "test-acct", &caps).with_market(INST, symbol.clone());
    let config = MarketCapsConfig {
        inventory: Lots::new(10),
        resting: Lots::new(10),
    };
    let market = PreTradeCaps::new().with_market(INST, config).unwrap();
    let mut h = handler_over(Registry::with_caps(market).with_lease_keys(keys));
    let dir = LeaseDir::new();

    // The first epoch's trustworthy resync seeds `INST` flat and views the account's open
    // orders; the owner's Start arms it under its market lease.
    epoch(&mut h, 0, 0, 1_000);
    let lease = MarketLease::acquire(&dir.0, "paradex", "test-acct", &symbol).unwrap();
    h.link
        .borrow_mut()
        .reg()
        .start(INST, Leases::market(lease))
        .unwrap();
    assert_eq!(refusal(&h), None);

    // The connection ends: an order of another engine may have been placed while it was down,
    // so the cancel everything is explicit, and stays so until the next epoch's resync.
    h.on_epoch_end(key(0));
    assert_eq!(refusal(&h), Some(CancelAllRefusal::NotViewed));
    for (n, body) in (5u64..).zip([
        conn(ConnState::Connecting),
        conn(ConnState::Open),
        conn(ConnState::Authenticated),
        ExecEvent::ResyncBegin {
            watermark: WallNs(2_000),
        },
    ]) {
        h.on_exec(env(n, 1, 2_000 + 10 * (n - 5), body));
        assert_eq!(refusal(&h), Some(CancelAllRefusal::NotViewed));
    }
    h.on_exec(env(9, 1, 2_040, ExecEvent::ResyncEnd));
    assert_eq!(refusal(&h), None);

    // The next end takes it away again.
    h.on_epoch_end(key(1));
    assert_eq!(refusal(&h), Some(CancelAllRefusal::NotViewed));
}

#[test]
fn on_a_session_of_two_connections_the_glue_reports_the_last_envelope_of_either() {
    let mut h = handler();
    let (zero, one) = (ConnKey { conn: 0, epoch: 0 }, ConnKey { conn: 1, epoch: 0 });

    // Connection 0's epoch, then an envelope of connection 1 received after its last.
    let last = epoch(&mut h, 0, 0, 1_000);
    assert_eq!(last, MonoNs(1_040));
    h.on_exec(env_on(5, one, 1_500, conn(ConnState::Open)));

    // Connection 0's end is reported at connection 1's later envelope: never earlier than its
    // own last.
    h.on_epoch_end(zero);
    assert_eq!(end_of(&h), Some(MonoNs(1_500)));

    // Connection 1's end, with nothing since, at the same instant.
    h.on_epoch_end(one);
    assert_eq!(end_of(&h), Some(MonoNs(1_500)));
}
