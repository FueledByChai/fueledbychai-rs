//! FBC-8ew's done line, its first half: the named suite runs on the conformance toy venue
//! through the suite macro (BT-502, design §6), `caps_truthful` and `commands_selfcontained`
//! each a test, and both pass. The checks are run directly too, to show what they probed: a
//! pass that probed nothing would prove nothing.

use fbc_conformance::suite::{self, Setup, Subject, Verdict};
use fbc_conformance::toy::{self, ToyFactory};
use fbc_core::{Secrets, VenueConfig};

/// What the toy's fixtures assume: its two instruments, no configuration, no credentials.
fn assumed() -> Setup {
    Setup {
        specs: toy::specs(),
        cfg: VenueConfig::new(),
        creds: Secrets::new(),
    }
}

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/conformance-toy"
);

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
    // The toy offers post-only, reduce-only, a dead-man timer and every cancel reference: none
    // of those is probed as absent.
    let absent = [
        "post_only is false",
        "reduce_only is false",
        "cancel_on_disconnect",
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
