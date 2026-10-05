//! Decision 0045: order entry reaches a gateway only through fbc-oms. An `Authorization` is
//! issued only inside `fbc-oms`, for a place, an amend, a batch, a cancel, a cancel-many or an
//! instrument cancel-all; it cannot be built, converted into, edited, cloned or submitted twice
//! outside it, and the gateway's unauthorized control path carries no order-affecting command.
//! Each case is a file under `tests/ui_authorization/` with its expected `.stderr`. Regenerate
//! the expected output with `TRYBUILD=overwrite cargo test -p fbc-oms --test compile_fail`
//! only when the pinned toolchain moves (0008).

/// Decision 0005's permits: an amend is built only through a `Live` permit and a cancel only
/// through a `Cancellable` one, which only the registry gives; neither stands for the other,
/// and what they build cannot be forged. Cases under `tests/ui_permits/`, regenerated as above.
#[test]
fn amends_and_cancels_are_built_only_through_their_permits() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_permits/*.rs");
}

#[test]
fn order_entry_needs_an_authorization_only_fbc_oms_issues() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_authorization/*.rs");
}
