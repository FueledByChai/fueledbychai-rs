//! Arming a market for the tests that build places and amends (decision 0012): lease names for
//! an account of its own per registry, so tests running at once never share a lock file, and
//! the leases each arming call takes.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::{AccountLease, InstrumentId, MarketLease, NamedLeaseError, NonceScope, OrderCaps};
use fbc_oms::{LeaseKeys, Leases, MarketEntry, Registry};

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

/// The owner's Start on `market`, with its leases: armed, Quoting.
pub fn start(reg: &mut Registry, market: InstrumentId) -> MarketEntry {
    let leases = leases(reg, market);
    reg.start(market, leases).unwrap()
}
