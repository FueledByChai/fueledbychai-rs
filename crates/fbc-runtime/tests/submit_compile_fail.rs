//! FBC-0ga, decisions 0045 and 0057: an order-entry session takes no place, amend, batch item,
//! cancel, cancel-many or cancel-all without fbc-oms's authorization. `ExecOrders::submit`
//! takes only an `Authorization`, which only fbc-oms issues (its own compile-fail cases prove
//! that), and `ExecOrders::submit_control` only a `ControlCommand`, which carries no
//! order-affecting command; neither takes a `VenueCommand`. Each case is a file under
//! `tests/ui_submit/` with its expected `.stderr`. Regenerate the expected output with
//! `TRYBUILD=overwrite cargo test -p fbc-runtime --test submit_compile_fail` only when the
//! pinned toolchain moves (0008).

#[test]
fn the_session_takes_no_order_affecting_command_without_an_authorization() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui_submit/*.rs");
}
