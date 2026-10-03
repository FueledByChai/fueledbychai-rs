//! Decision 0004: the sealed types refuse misuse at compile time, proven case by case by the
//! files under `tests/ui/` (the four clocks cannot be mixed), `tests/ui_units/` (the unit
//! seals), `tests/ui_ids/` (the id seals) and `tests/ui_fee/` (the fee seal) and their expected
//! `.stderr`. Regenerate the expected output with
//! `TRYBUILD=overwrite cargo test -p fbc-core --test compile_fail` only when the pinned
//! toolchain moves (0008).

#[test]
fn cross_kind_time_subtraction_does_not_compile() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}

/// A negative [`fbc_core::Lots`] cannot be built (its field is private), and `Ticks`, `Lots`
/// and `SignedLots` have no `+`, `-` or unary `-` operator that could wrap in a release build;
/// their arithmetic is `checked_*` only.
#[test]
fn unit_seals_do_not_compile() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_units/*.rs");
}

/// `ClientOrderId`, `VenueOrderId`, `FillId` and `DecodeScope` cannot be built outside
/// `fbc-core`, a lent `DecodeScope` cannot escape the dispatch callback, and a `CidMint` cannot
/// exist without a held `NamespaceLease`, which only `NamespaceLease::acquire` makes (decision
/// 0004, design §4.3).
#[test]
fn id_seals_do_not_compile() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_ids/*.rs");
}

/// A `Fee` cannot be built outside `fbc-core` (not from a `Money`, not through the crate-private
/// constructor `DecodeScope::fee` uses), cannot be edited, and has no `abs`, no negation and no
/// conversion from `f64` (decision 0004, design §4.2).
#[test]
fn fee_seal_does_not_compile() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_fee/*.rs");
}
