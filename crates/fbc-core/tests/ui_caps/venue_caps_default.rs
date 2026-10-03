use fbc_core::VenueCaps;

fn main() {
    // No capability type has a Default (decision 0003): every field is declared.
    let _ = VenueCaps::default();
}
