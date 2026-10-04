//! Decision 0003: what a venue can do is data, and every field is mandatory. The synthetic test
//! venue in `tests/common/` declares every field of `VenueCaps` and of each type it is made of,
//! by name; it compiles only because nothing is left out, since no capability type has a
//! `Default` to fill a gap (`tests/ui_caps/` proves a literal missing a field, and
//! `VenueCaps::default()`, do not compile). Its values describe no real venue. Order and fill
//! capabilities come together in one `exec` block (decision 0015); the market-data-only venue
//! there declares `exec: None` and nothing about fills, and `tests/ui_caps/` proves neither half
//! can be written without the other.

mod common;

use std::fs;
use std::path::Path;

use common::{market_data_only_caps, synthetic_caps};
use fbc_core::{
    AssetSym, Batch, Channel, Feature, FeedSource, InstrumentId, LimitScope, Money, Namespace,
    NonceScope, OpKind, OrderKindTag, RateCharge, Readiness, RefKind, SpeedBumpScope, Support,
    TifTag, dispatch,
};

#[test]
fn the_synthetic_venue_declares_every_capability() {
    let caps = synthetic_caps();
    let exec = caps
        .exec
        .as_ref()
        .expect("the synthetic venue takes orders");
    let order = &exec.order;
    assert!(order.kinds.contains(OrderKindTag::Market));
    assert!(!order.tifs.contains(TifTag::Fok));
    assert!(order.channels.contains(Channel::Rpi));
    assert!(
        order
            .flag_conflicts
            .contains(&(Feature::PostOnly, Feature::ReduceOnly))
    );
    let amend = order.amend.expect("the synthetic venue amends");
    assert_eq!(
        (amend.when_partially_filled, amend.keeps_priority),
        (false, None)
    );
    assert!(order.query_refs.contains(RefKind::Client));
    assert!(!order.query_refs.contains(RefKind::Venue));
    assert_eq!(order.batch_place, Some(Batch { max_items: 10 }));
    assert_eq!(order.cancel_all_instrument, Support::Unsupported);
    assert_eq!(order.nonce_scope, NonceScope::PerAccountMonotonic);
    assert_eq!(
        caps.matching.speed_bump.map(|bump| bump.applies_to),
        Some(SpeedBumpScope::TakersOnly)
    );
    assert_eq!(caps.md.books.len(), 2);
    // Mark and index are subscribable feeds, so the caps say where each comes from.
    assert_eq!(
        (caps.md.mark, caps.md.index),
        (FeedSource::Stream, FeedSource::Poll)
    );
    assert!(caps.md.books[1].includes_channels.contains(Channel::Rpi));
    assert!(!caps.md.books[0].includes_channels.contains(Channel::Rpi));
    assert_eq!(caps.limits[0].ops.iter().count(), 3);
    // Decision 0018: limits per instrument, per connection, and on opening connections, each
    // counting the charges for its operations (a per-pair one only when they name the pair).
    let scopes = caps.limits.iter().map(|l| l.scope).collect::<Vec<_>>();
    assert_eq!(
        scopes,
        [
            LimitScope::Account,
            LimitScope::Ip,
            LimitScope::Pair,
            LimitScope::Connection,
            LimitScope::Ip,
        ]
    );
    let (pair, conn, connect) = (&caps.limits[2], &caps.limits[3], &caps.limits[4]);
    let inst = Some(InstrumentId::new(1));
    assert!(pair.counts(&RateCharge::one(OpKind::Amend, inst)));
    assert!(!pair.counts(&RateCharge::one(OpKind::Amend, None)));
    assert!(conn.counts(&RateCharge::one(OpKind::Control, None)));
    assert!(connect.counts(&RateCharge::one(OpKind::Connect, None)));
    assert!(!connect.counts(&RateCharge::one(OpKind::Rest, None)));
    assert_eq!(caps.readiness_ceiling, Readiness::Paper);
    // Capabilities are plain data: a clone is equal, and a debug print names the values.
    assert_eq!(caps.clone(), caps);
    assert!(format!("{caps:?}").contains("PositiveIsRebate"));
}

#[test]
fn the_declared_fee_sign_and_client_id_format_drive_the_decode_scope() {
    // FBC-75u: dispatch takes the venue's caps, so the scope decodes under exactly the fee sign
    // and client-id format the venue declares. The synthetic venue reports a rebate as a
    // positive amount; as a Fee it is a negative cost.
    // (tests/client_ids.rs proves the format half with minted ids.)
    let caps = synthetic_caps();
    let usdc = AssetSym::new("USDC").unwrap();
    let fee = dispatch(&caps, Namespace::new(2), |scope| scope.fee(150_000, usdc));
    assert_eq!(fee.unwrap().cost(), Money::new(-150_000, usdc));
}

#[test]
fn a_market_data_only_venue_declares_no_order_or_fill_capability() {
    let md_only = market_data_only_caps();
    assert!(md_only.exec.is_none());
    assert_ne!(md_only, synthetic_caps());
    // Nothing about fills or fees survives anywhere in the declaration.
    let printed = format!("{md_only:?}");
    for claim in ["fills", "fee_sign", "FillSource", "PositiveIs", "client_id"] {
        assert!(!printed.contains(claim), "{claim} in {printed}");
    }
    // What it does declare is still stated in full.
    assert_eq!(md_only.md, synthetic_caps().md);
    assert_eq!(md_only.matching, synthetic_caps().matching);
    assert!(Readiness::Record < Readiness::Shadow);
    assert!(Readiness::Shadow < Readiness::Paper);
    assert!(Readiness::Paper < Readiness::Live);
}

#[test]
fn no_capability_or_instrument_type_has_a_default_or_is_non_exhaustive() {
    // Decision 0003: a Default would let an adapter skip a field, and #[non_exhaustive] would
    // let one compile without declaring a new one. The compile-fail tests prove it for
    // VenueCaps; this holds the line for every type in the two modules.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for module in ["caps.rs", "instrument.rs"] {
        let text = fs::read_to_string(src.join(module)).unwrap();
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap();
            assert!(
                !code.contains("Default") && !code.contains("non_exhaustive"),
                "{module}:{}: {}",
                n + 1,
                line.trim()
            );
        }
    }
}
