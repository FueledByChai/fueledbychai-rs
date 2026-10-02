//! Decision 0004: the four clocks cannot be mixed. Every cross-kind subtraction is a compile
//! error, proven case by case by the files under `tests/ui/` and their expected `.stderr`.
//! Regenerate the expected output with `TRYBUILD=overwrite cargo test -p fbc-core --test
//! compile_fail` only when the pinned toolchain moves (0008).

#[test]
fn cross_kind_time_subtraction_does_not_compile() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
