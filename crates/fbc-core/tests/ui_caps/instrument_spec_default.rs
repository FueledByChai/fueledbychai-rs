use fbc_core::InstrumentSpec;

fn main() {
    // An instrument spec has no Default: no tick, lot or funding interval is ever guessed.
    let _ = InstrumentSpec::default();
}
