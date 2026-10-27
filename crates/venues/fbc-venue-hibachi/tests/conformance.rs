//! FBC-2kmp: fbc-venue-hibachi runs the named conformance suite. Hibachi declares no order entry
//! (`exec: None`), so the order-entry and fill checks skip by name; the market-data checks run
//! on the `orderbook` channel's hand-built cases in `fixtures/hibachi/conformance/`, and
//! `continuity` skips by name for a channel declared `Continuity::Unsequenced`, whose gaps the
//! bounds check finds (decision 0129; `tests/md_book.rs`).

mod common;
#[path = "conformance/setup.rs"]
mod setup;

use fbc_venue_hibachi::Hibachi;
use setup::assumed;

fbc_conformance::suite! {
    factory: Hibachi,
    fixtures: "../../../fixtures/hibachi/conformance",
    setup: assumed,
}

/// Which checks run and which skip, by name: every market-data check the suite has for a
/// `Continuity::Unsequenced` JSON channel passes, and nothing that needs order entry runs.
#[test]
fn the_market_data_checks_pass_and_the_order_entry_checks_skip_by_name() {
    use fbc_conformance::suite::{self, Subject, Verdict};
    let subject = Subject::new(&Hibachi, setup::FIXTURES, assumed).unwrap();
    let passed = |v: Verdict| matches!(v, Verdict::Passed { .. });
    let skipped = |v: Verdict| matches!(v, Verdict::Skipped { .. });
    for (name, check) in [
        (
            "book_channels",
            suite::book_channels as fn(&Subject<'_>) -> _,
        ),
        ("no_exch_ts_synthesized", suite::no_exch_ts_synthesized),
        ("subscriptions_idempotent", suite::subscriptions_idempotent),
        ("legacy_symbols", suite::legacy_symbols),
        ("price_grid", suite::price_grid),
        ("caps_truthful", suite::caps_truthful),
    ] {
        let verdict = check(&subject).unwrap_or_else(|f| panic!("{name}: {f}"));
        assert!(passed(verdict.clone()), "{name}: {verdict:?}");
    }
    for (name, check) in [
        ("continuity", suite::continuity as fn(&Subject<'_>) -> _),
        ("decoder_deterministic", suite::decoder_deterministic),
        ("commands_selfcontained", suite::commands_selfcontained),
        ("signing_golden", suite::signing_golden),
        ("fee_sign", suite::fee_sign),
        ("ids_roundtrip", suite::ids_roundtrip),
        ("encode_deterministic", suite::encode_deterministic),
    ] {
        let verdict = check(&subject).unwrap_or_else(|f| panic!("{name}: {f}"));
        assert!(skipped(verdict.clone()), "{name}: {verdict:?}");
    }
}
