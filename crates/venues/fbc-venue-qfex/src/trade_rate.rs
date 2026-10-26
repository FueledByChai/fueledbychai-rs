//! The authenticated trading WebSocket has separate general and cancel budgets per provider
//! user. The public MDS has no published numeric budget (decision 0123).

use std::time::Duration;

use fbc_core::{LimitScope, OpKind, RateLimit, TagSet};

/// QFEX publishes 12,000 weight units per 60 seconds in each bucket. Tenths keep its 0.1-unit
/// messages integral. The consumer binds these limits to one attested provider-user key with
/// `RateLimiter::for_user`, shared by primary and subaccounts in one owner process.
pub(crate) fn limits() -> [RateLimit; 2] {
    let per = Duration::from_secs(60);
    [
        RateLimit {
            scope: LimitScope::User,
            ops: TagSet::of(&[
                OpKind::Place,
                OpKind::Amend,
                OpKind::Query,
                OpKind::Subscribe,
                // Control includes both general messages and cancel-on-disconnect. Charge
                // both buckets until the wire operation has its own rate kind.
                OpKind::Control,
            ]),
            per,
            units: 120_000,
        },
        RateLimit {
            scope: LimitScope::User,
            ops: TagSet::of(&[OpKind::Cancel, OpKind::CancelAll, OpKind::Control]),
            per,
            units: 120_000,
        },
    ]
}
