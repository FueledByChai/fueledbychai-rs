use std::num::NonZeroU32;
use std::time::Duration;

use fbc_core::{
    ConnKey, LimitScope, OpKind, RateCharge, TrafficClass, VenueConfig, VenueFactory, Via,
};
use fbc_runtime::{BucketKey, RateError, RateLimiter, RateUserKey, Request, SafetyReserve};
use fbc_venue_qfex::{KEY_MDS_URL, Qfex};
use tokio::time::Instant;

fn config() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg
}

fn request(op: OpKind, weight: u32, class: TrafficClass) -> Request {
    Request {
        charge: RateCharge {
            op,
            inst: None,
            weight: NonZeroU32::new(weight).unwrap(),
        },
        via: Via::Frame,
        class,
    }
}

#[test]
fn private_buckets_share_across_accounts_but_not_provider_users() {
    let cfg = config();
    let caps = Qfex.caps(&cfg).unwrap();
    assert!(caps.limits.is_empty());
    assert!(caps.exec.is_none());
    let limits = caps.exec_limits.as_ref().unwrap();
    assert_eq!(limits.len(), 2);
    for limit in limits {
        assert_eq!(limit.scope, LimitScope::User);
        assert_eq!(limit.per, Duration::from_secs(60));
        assert_eq!(limit.units, 120_000); // tenths of one QFEX weight unit
    }
    assert!(limits[0].ops.contains(OpKind::Place));
    assert!(limits[0].ops.contains(OpKind::Query));
    assert!(limits[0].ops.contains(OpKind::Subscribe));
    assert!(!limits[0].ops.contains(OpKind::Cancel));
    assert!(limits[1].ops.contains(OpKind::Cancel));
    assert!(limits[1].ops.contains(OpKind::CancelAll));
    assert!(!limits[1].ops.contains(OpKind::Place));
    assert!(
        limits
            .iter()
            .all(|limit| limit.ops.contains(OpKind::Control))
    );

    let reserve = SafetyReserve::percent(20).unwrap();
    assert_eq!(
        RateLimiter::new(limits, reserve).unwrap_err(),
        RateError::UserKeyRequired
    );
    let primary = RateLimiter::for_user(RateUserKey::new(0xb63f_0001), limits, reserve).unwrap();
    let subaccount = RateLimiter::for_user(RateUserKey::new(0xb63f_0001), limits, reserve).unwrap();
    let other_user = RateLimiter::for_user(RateUserKey::new(0xb63f_0002), limits, reserve).unwrap();
    let now = Instant::now();
    let conn = ConnKey { conn: 1, epoch: 1 };

    primary
        .charge(
            now,
            conn,
            &[request(OpKind::Place, 96_000, TrafficClass::Normal)],
        )
        .unwrap();
    assert!(
        subaccount
            .charge(
                now,
                conn,
                &[request(OpKind::Place, 1, TrafficClass::Normal)]
            )
            .is_err()
    );
    subaccount
        .charge(
            now,
            conn,
            &[request(OpKind::Place, 24_000, TrafficClass::Safety)],
        )
        .unwrap();
    assert_eq!(primary.used(now, 0, BucketKey::Shared), 120_000);
    // The cancel bucket still has its own Safety room on the same provider user.
    subaccount
        .charge(
            now,
            conn,
            &[request(OpKind::Cancel, 1, TrafficClass::Safety)],
        )
        .unwrap();
    assert_eq!(primary.used(now, 1, BucketKey::Shared), 1);
    other_user
        .charge(
            now,
            conn,
            &[request(OpKind::Place, 1, TrafficClass::Normal)],
        )
        .unwrap();
    assert_eq!(other_user.used(now, 0, BucketKey::Shared), 1);
    assert_eq!(
        RateLimiter::for_user(
            RateUserKey::new(0xb63f_0001),
            limits,
            SafetyReserve::percent(0).unwrap()
        )
        .unwrap_err(),
        RateError::UserBudgetMismatch
    );
}
