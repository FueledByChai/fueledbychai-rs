//! Arming a market for the tests that build places and amends (decision 0012): lease names for
//! an account of its own per registry, so tests running at once never share a lock file, and
//! the leases each arming call takes.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_core::{
    AccountLease, InstrumentId, MarketLease, MonoNs, NamedLeaseError, NonceScope, OrderCaps,
    SignedLots, SnapshotSource, WallNs,
};
use fbc_oms::{
    LadderConfig, LeaseKeys, Leases, MarketEntry, OrderKey, Registry, ResyncReport, ResyncSnapshot,
};

use crate::common::{lease_dir, order_caps, symbol};

/// The synthetic venue's name.
pub const VENUE: &str = "synthetic";

/// The symbol of each test market: `SYN-<id>-PERP`.
pub fn wire(market: InstrumentId) -> String {
    format!("SYN-{}-PERP", market.get())
}

/// The test venue's order caps with the nonce scope `scope`.
pub fn scoped(scope: NonceScope) -> OrderCaps {
    OrderCaps {
        nonce_scope: scope,
        ..order_caps()
    }
}

/// Lease names for an account no other registry of this test binary uses, whose nonce scope is
/// `scope`, naming the test markets 1 to 4.
pub fn lease_keys(scope: NonceScope) -> LeaseKeys {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let account = format!("acct-{}", NEXT.fetch_add(1, Ordering::Relaxed));
    (1..=4).map(InstrumentId::new).fold(
        LeaseKeys::new(VENUE, &account, &scoped(scope)),
        |keys, m| keys.with_market(m, symbol(&wire(m))),
    )
}

/// `reg` under lease names of its own, with per-account nonces (the strictest case).
pub fn named(reg: Registry) -> Registry {
    reg.with_lease_keys(lease_keys(NonceScope::PerAccountMonotonic))
}

/// The market lease on `market` under `reg`'s names.
pub fn market_lease(reg: &Registry, market: InstrumentId) -> MarketLease {
    let keys = reg.lease_keys().expect("the registry is named");
    MarketLease::acquire(
        &lease_dir(),
        keys.venue(),
        keys.account(),
        &symbol(&wire(market)),
    )
    .unwrap()
}

/// The account lease under `reg`'s names; `None` while it is held (by the registry).
pub fn account_lease(reg: &Registry) -> Option<AccountLease> {
    let keys = reg.lease_keys().expect("the registry is named");
    match AccountLease::acquire(&lease_dir(), keys.venue(), keys.account()) {
        Ok(lease) => Some(lease),
        Err(NamedLeaseError::Held { .. }) => None,
        Err(other) => panic!("cannot take the account lease: {other}"),
    }
}

/// Every lease arming `market` under `reg`'s names takes.
pub fn leases(reg: &Registry, market: InstrumentId) -> Leases {
    let lease = Leases::market(market_lease(reg, market));
    match account_lease(reg) {
        Some(account) => lease.with_account(account),
        None => lease,
    }
}

/// Seeds `positions` on `reg` as the first trustworthy resync does (decision 0055), the only
/// seed a market arms from outside a declared testnet run (decision 0067): a snapshot of no
/// orders, requested at monotonic 0 with watermark 0, so every fill after it counts.
pub fn seed(reg: &mut Registry, positions: &[(InstrumentId, i64)]) -> ResyncReport {
    let cfg = LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap();
    let caps = OrderCaps {
        snapshot_source: SnapshotSource::Trustworthy,
        ..order_caps()
    };
    let snap = ResyncSnapshot {
        watermark: WallNs(0),
        requested_at: MonoNs(0),
        orders: vec![],
        positions: positions.iter().map(|&(m, p)| (m, SignedLots(p))).collect(),
    };
    let report = reg
        .resync(
            &cfg,
            &caps,
            &snap,
            OrderKey {
                venue: None,
                ingest: 0,
            },
        )
        .unwrap();
    assert!(!report.untrustworthy);
    report
}

/// The owner's Start on `market`, with its leases: armed, Quoting.
pub fn start(reg: &mut Registry, market: InstrumentId) -> MarketEntry {
    let leases = leases(reg, market);
    reg.start(market, leases).unwrap()
}
