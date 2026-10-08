//! FBC-8ew's, FBC-onw's, FBC-whw's, FBC-2re's and FBC-vmw's done lines, their first halves: the
//! named suite runs on the conformance toy venue through the suite macro with its fixture
//! directory (BT-502, design §6), `caps_truthful`, `commands_selfcontained`, `signing_golden`,
//! `legacy_symbols`, `fee_sign`, `liquidity_reported`, `position_signed`,
//! `decoder_deterministic`, `encode_deterministic`, `ids_roundtrip`, `restart_cid`,
//! `price_grid`, `continuity`, `subscriptions_idempotent`, `no_exch_ts_synthesized` and
//! `book_channels` each a test, and all pass; `continuity`'s longer-block sub-case reports
//! itself skipped by name for the toy's text protocol. The checks are run directly too, to show what they probed: a
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

#[test]
fn encode_deterministic_encodes_every_golden_and_built_command_twice_on_the_toy() {
    let probed = probed(suite::encode_deterministic);
    assert_eq!(
        probed,
        [
            "golden place",
            "golden place-batch",
            "golden amend",
            "golden cancel",
            "golden cancel-batch",
            "the plainest order",
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
fn ids_roundtrip_round_trips_minted_ids_and_reads_every_java_era_id_as_unparseable() {
    assert_eq!(
        probed(suite::ids_roundtrip),
        [
            "OrderCaps.client_id is Alnum { max_len: 32, charset: Alphanumeric }: 9 minted ids",
            "ids_roundtrip/java_era.txt: 5 Java-era ids",
        ]
    );
}

#[test]
fn restart_cid_restarts_the_mint_above_every_id_of_ours_the_toys_resync_shows() {
    assert_eq!(
        probed(suite::restart_cid),
        ["restart_cid/resting.frames: 2 of ours resting, 4 shown, restarted above 8"]
    );
}

#[test]
fn price_grid_quantizes_and_sends_every_boundary_price_on_both_of_the_toys_instruments() {
    let probed = probed(suite::price_grid);
    assert_eq!(probed.len(), 2, "{probed:#?}");
    for (p, symbol) in probed.iter().zip(["TOYA-PERP", "TOYB-PERP"]) {
        assert_eq!(
            *p,
            format!(
                "{symbol}: 45 model prices quantized to 54 order prices, 54 post-only orders sent"
            )
        );
    }
}

/// Why the toy's `rpi_book` is skipped by every market-data check.
const ANCHORED: &str = "rpi_book: skipped: BookCaps.rest_anchor is true: its snapshot comes in \
                        an HTTP answer, which the suite does not carry yet (FBC-fhk4)";

#[test]
fn continuity_detects_every_marked_break_and_skips_the_longer_block_sub_case_by_name() {
    assert_eq!(
        probed(suite::continuity),
        [
            ANCHORED,
            "continuity/book.frames: 3 of 11 frames break the sequence",
            "longer_block: skipped: MdCaps.encoding is Text: its frames carry no binary block",
        ]
    );
}

#[test]
fn subscriptions_idempotent_sends_the_toys_book_once_per_epoch() {
    assert_eq!(
        probed(suite::subscriptions_idempotent),
        [
            ANCHORED,
            "book: 2 subscriptions sent once in each of 2 epochs ([1, 1] effects), nothing for \
             the same set again",
        ]
    );
}

#[test]
fn no_exch_ts_synthesized_judges_every_event_of_a_frame_without_a_timestamp_on_the_toy() {
    assert_eq!(
        probed(suite::no_exch_ts_synthesized),
        [
            ANCHORED,
            "no_exch_ts_synthesized/book.frames: 6 events from frames without a timestamp",
        ]
    );
}

#[test]
fn book_channels_holds_the_toys_book_to_public_liquidity() {
    assert_eq!(
        probed(suite::book_channels),
        [ANCHORED, "book_channels/book.frames: shows [Public]"]
    );
}
