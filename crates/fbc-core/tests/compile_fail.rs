//! Decision 0004: the sealed types refuse misuse at compile time, proven case by case by the
//! files under `tests/ui/` (the four clocks cannot be mixed), `tests/ui_units/` (the unit
//! seals), `tests/ui_ids/` (the id seals), `tests/ui_fee/` (the fee seal), `tests/ui_caps/`
//! (decision 0003: capabilities and specs with every field mandatory) and `tests/ui_states/`
//! (decision 0005: no terminal state for an unknown order, no request without a deadline),
//! `tests/ui_stamps/` (decision 0034: path stamps give a codec no time) and their expected
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

/// A `VenueCaps` (or `FillCaps`) literal missing one field does not compile, nor an `AmendCaps`
/// or `CancelBatch` that does not say which references it can name (0032), `VenueCaps` and
/// `InstrumentSpec` have no `default()`, and a `VenueSymbol` cannot be built outside `fbc-core`
/// (decision 0003, design §4.4, §4.5). Order capabilities without fill capabilities, and fill
/// capabilities without an exec block, cannot be written (decision 0015).
#[test]
fn caps_and_specs_declare_every_field() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_caps/*.rs");
}

/// `VenueOrderState::Rejected` takes a `TerminalReject`, which refuses `RejectKind::NotFound`
/// and cannot be built as a literal: not knowing an order is never terminal (decision 0005).
/// An RPC frame and an HTTP request cannot be asked for without a timeout, so every
/// order-entry request reaches Unknown when unanswered, nor without its rate charge (decision
/// 0018).
#[test]
fn a_not_found_rejection_is_not_a_terminal_state() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_states/*.rs");
}

/// A codec marks its stages through `PathStamps` and learns no time from it: it cannot reach the
/// runtime's recorder through the handle, cannot ask it whether it records (so it cannot tell
/// live from replay), and neither `PathStamps` nor `PathRecorder::mark` returns the instant a
/// mark was recorded at (decision 0034).
#[test]
fn path_stamps_give_a_codec_no_time() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_stamps/*.rs");
}
