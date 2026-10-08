//! FBC-8ew's, FBC-onw's and FBC-whw's done lines, their first halves: the named suite runs on
//! the conformance toy venue through the suite macro with its fixture directory (BT-502,
//! design §6), `caps_truthful`, `commands_selfcontained`, `signing_golden`, `legacy_symbols`,
//! `fee_sign`, `liquidity_reported`, `position_signed` and `decoder_deterministic` each a
//! test, and all pass. The checks are run directly too, to show what they probed: a
//! pass that probed nothing would prove nothing.

mod toy_setup;

use fbc_conformance::suite::{self, Subject, Verdict};
use fbc_conformance::toy::ToyFactory;
use toy_setup::{FIXTURES, assumed};

fbc_conformance::suite! {
    factory: ToyFactory,
    fixtures: "../../fixtures/conformance-toy",
    setup: assumed,
}

/// What `check` probed on the toy, which must pass.
fn probed(check: fn(&Subject<'_>) -> Result<Verdict, suite::Failure>) -> Vec<String> {
    let subject = Subject::new(&ToyFactory, FIXTURES, assumed).unwrap();
    match check(&subject) {
        Ok(Verdict::Passed { probed, .. }) => probed,
        other => panic!("{other:?}"),
    }
}

#[test]
fn caps_truthful_probes_every_absence_and_the_declared_flag_conflict_on_the_toy() {
    let probed = probed(suite::caps_truthful);
    for name in [
        "control: a placement",
        "control: a batch placement",
        "control: an amend",
        "control: a cancel",
        "control: a batch cancel",
        "control: a query",
        "control: protection (arm)",
        "control: protection (disarm)",
        "OrderCaps.cancel_on_disconnect has no dead-man timer (refresh)",
        "OrderCaps.kinds lacks Market",
        "OrderCaps.tifs lacks Fok",
        "OrderCaps.tifs lacks Fok (a batch item)",
        "OrderCaps.tifs lacks Fok (an amend)",
        "OrderCaps.channels lacks Rpi",
        "OrderCaps.flag_conflicts has (PostOnly, Ioc)",
        "OrderCaps.flag_conflicts has (PostOnly, Ioc) (a batch item)",
        "OrderCaps.flag_conflicts has (PostOnly, Ioc) (an amend)",
        "Batch.max_items is 4",
        "AmendCaps.refs lacks [Client]",
        "CancelBatch.refs lacks [Client]",
        "CancelBatch.max_items is 4",
        "OrderCaps.query_refs lacks [Client]",
        "OrderCaps.cancel_all_account is Unsupported",
        "OrderCaps.cancel_all_instrument is Native: never widened",
    ] {
        assert!(
            probed.iter().any(|p| p == name),
            "{name} not probed: {probed:#?}"
        );
    }
    // The toy offers post-only, reduce-only, per-connection protection and every cancel
    // reference: none of those is probed as absent.
    let absent = [
        "post_only is false",
        "reduce_only is false",
        "cancel_on_disconnect has no protection",
        "cancel_refs lacks",
    ];
    for name in absent {
        assert!(
            !probed.iter().any(|p| p.contains(name)),
            "{name} probed: {probed:#?}"
        );
    }
}

#[test]
fn commands_selfcontained_encodes_every_declared_reference_on_the_toy() {
    let probed = probed(suite::commands_selfcontained);
    assert_eq!(
        probed,
        [
            "AmendCaps.refs has Venue: an amend",
            "OrderCaps.cancel_refs has Venue: a cancel",
            "OrderCaps.cancel_refs has Client: a cancel",
            "CancelBatch.refs has Venue: a batch cancel",
            "CancelBatch.refs has PlacementNonce: a batch cancel",
            "OrderCaps.query_refs has Venue: a query",
            "OrderCaps.query_refs has PlacementNonce: a query",
        ]
    );
}

#[test]
fn signing_golden_compares_every_golden_command_on_the_toy() {
    assert_eq!(
        probed(suite::signing_golden),
        [
            "golden place",
            "golden place-batch",
            "golden amend",
            "golden cancel",
            "golden cancel-batch",
        ]
    );
}

#[test]
fn legacy_symbols_reads_every_ticker_in_the_toys_fixture() {
    assert_eq!(
        probed(suite::legacy_symbols),
        [
            "TOYA/USDT: TOYA/USDT Perpetual",
            "TOYB/USDT: TOYB/USDT Perpetual",
        ]
    );
}

#[test]
fn fee_sign_judges_the_fee_of_every_fill_in_both_cases_on_the_toy() {
    assert_eq!(
        probed(suite::fee_sign),
        [
            "fee_sign/rebate.frames: 2 fills",
            "fee_sign/paid.frames: 1 fill"
        ]
    );
}

#[test]
fn liquidity_reported_judges_every_fill_in_both_cases_on_the_toy() {
    assert_eq!(
        probed(suite::liquidity_reported),
        [
            "liquidity_reported/maker.frames: 2 fills",
            "liquidity_reported/taker.frames: 1 fill",
        ]
    );
}

#[test]
fn position_signed_judges_every_position_in_both_cases_on_the_toy() {
    assert_eq!(
        probed(suite::position_signed),
        [
            "position_signed/long.frames: 2 positions",
            "position_signed/short.frames: 1 position",
        ]
    );
}

#[test]
fn decoder_deterministic_decodes_every_case_of_the_toys_fixtures_twice() {
    assert_eq!(
        probed(suite::decoder_deterministic),
        [
            "fee_sign/paid.frames: 1 line",
            "fee_sign/rebate.frames: 2 lines",
            "liquidity_reported/maker.frames: 2 lines",
            "liquidity_reported/taker.frames: 1 line",
            "position_signed/long.frames: 5 lines",
            "position_signed/short.frames: 4 lines",
            "decoder_deterministic/orders.frames: 5 lines",
        ]
    );
}
