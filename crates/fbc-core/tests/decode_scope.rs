//! Decision 0004: venue order ids, fill ids and fees are built only through the `DecodeScope`
//! the core's dispatch lends to a codec callback; tests use the same scope. FBC-75u: dispatch
//! takes the venue's `VenueCaps`, so the scope applies the fee sign and client-id format the
//! venue declares in its exec block (decision 0015), and a market-data-only venue
//! (`exec: None`) is dispatched without a namespace, reads no client id and decodes no fee.

mod common;

use common::{exec_caps, market_data_only_caps, uuid_caps};
use fbc_core::{
    AssetSym, Charset, CidMatch, ClientIdFormat, FeeError, IdError, MAX_VENUE_ID_LEN, Money,
    Namespace, VenueFeeSign, dispatch, dispatch_market_data,
};

const HIBACHI_LIKE: ClientIdFormat = ClientIdFormat::Alnum {
    max_len: 32,
    charset: Charset::Alphanumeric,
};

fn usdc() -> AssetSym {
    AssetSym::new("USDC").unwrap()
}

#[test]
fn the_scope_builds_venue_ids_from_the_wire_text() {
    let caps = exec_caps(HIBACHI_LIKE, VenueFeeSign::PositiveIsCost);
    let (vid, fid) = dispatch(&caps, Namespace::new(3), |scope| {
        (
            scope.venue_order_id("1759363200000201030"),
            scope.fill_id("8812"),
        )
    });
    assert_eq!(vid.unwrap().as_str(), "1759363200000201030");
    assert_eq!(fid.unwrap().as_str(), "8812");
}

#[test]
fn the_scope_refuses_empty_and_overlong_venue_ids() {
    let over = "7".repeat(MAX_VENUE_ID_LEN + 1);
    dispatch(&uuid_caps(), Namespace::new(3), |scope| {
        assert_eq!(scope.venue_order_id(""), Err(IdError::Empty));
        assert_eq!(scope.fill_id(""), Err(IdError::Empty));
        assert!(matches!(
            scope.venue_order_id(&over),
            Err(IdError::TooLong { len: 97, .. })
        ));
        assert!(matches!(scope.fill_id(&over), Err(IdError::TooLong { .. })));
    });
}

#[test]
fn the_scope_decodes_client_ids_against_its_namespace() {
    dispatch(&uuid_caps(), Namespace::new(3), |scope| {
        assert_eq!(
            scope.client_order_id("f47ac10b-58cc-4372-a567-0e02b2c3d479"),
            CidMatch::Unparseable
        );
        assert_eq!(
            scope.client_order_id("1759363200001"),
            CidMatch::Unparseable
        );
        assert!(format!("{scope:?}").contains("DecodeScope"));
    });
}

#[test]
fn a_fee_decoded_under_a_rebate_venues_caps_is_a_negative_cost() {
    // The done line: the synthetic venue declares PositiveIsRebate, so a positive raw amount is
    // a rebate, which as a Fee is a negative cost and a gain to P&L. The sign is the caps' own,
    // read by dispatch; nothing else can supply one.
    let caps = exec_caps(HIBACHI_LIKE, VenueFeeSign::PositiveIsRebate);
    let fee = dispatch(&caps, Namespace::new(3), |scope| scope.fee(3_000, usdc())).unwrap();
    assert_eq!(fee.cost(), Money::new(-3_000, usdc()));
    assert_eq!(fee.pnl(), Money::new(3_000, usdc()));
    // The same amount under a cost venue's caps is a fee we paid.
    let caps = exec_caps(HIBACHI_LIKE, VenueFeeSign::PositiveIsCost);
    let fee = dispatch(&caps, Namespace::new(3), |scope| scope.fee(3_000, usdc())).unwrap();
    assert_eq!(fee.cost(), Money::new(3_000, usdc()));
}

#[test]
fn market_data_dispatch_takes_no_namespace_and_decodes_under_the_same_caps() {
    // A market-data session holds no engine namespace. Venue ids and symbols decode as in an
    // exec scope, and a venue with an exec block still has its declared fee sign.
    let caps = exec_caps(HIBACHI_LIKE, VenueFeeSign::PositiveIsRebate);
    dispatch_market_data(&caps, |scope| {
        assert_eq!(
            scope.venue_symbol("SYN-USD-PERP").unwrap().as_wire(),
            "SYN-USD-PERP"
        );
        assert_eq!(scope.venue_order_id("V-1").unwrap().as_str(), "V-1");
        assert_eq!(
            scope.fee(3_000, usdc()).unwrap().cost(),
            Money::new(-3_000, usdc())
        );
    });
}

#[test]
fn a_market_data_only_venue_decodes_ids_but_refuses_every_fee() {
    // exec: None declares no fee sign, so no fee is decoded under a guessed one, whichever
    // dispatch lends the scope and whatever the amount; the error names the missing block.
    let md_only = market_data_only_caps();
    for raw in [0, 3_000, -3_000, i128::MAX, i128::MIN] {
        let by_md = dispatch_market_data(&md_only, |scope| scope.fee(raw, usdc()));
        let by_exec = dispatch(&md_only, Namespace::new(3), |scope| scope.fee(raw, usdc()));
        assert_eq!(by_md, Err(FeeError::NoExecBlock), "{raw}");
        assert_eq!(by_exec, Err(FeeError::NoExecBlock), "{raw}");
    }
    let refused = FeeError::NoExecBlock.to_string();
    assert!(refused.contains("exec: None"), "{refused}");
    assert!(refused.contains("no fee sign"), "{refused}");
    // Market data itself still decodes.
    let symbol = dispatch_market_data(&md_only, |scope| scope.venue_symbol("SYN-USD-PERP"));
    assert_eq!(symbol.unwrap().as_wire(), "SYN-USD-PERP");
}
