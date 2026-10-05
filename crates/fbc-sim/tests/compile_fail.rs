//! FBC-30g: the queue model's configuration has no `Default`, so every bracket is the caller's
//! choice (0009). The case is `tests/ui/config_default.rs` with its expected `.stderr`;
//! regenerate that with `TRYBUILD=overwrite cargo test -p fbc-sim --test compile_fail` only
//! when the pinned toolchain moves (0008).

#[test]
fn the_queue_config_has_no_default() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
