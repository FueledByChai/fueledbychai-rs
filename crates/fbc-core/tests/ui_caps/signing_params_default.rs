use fbc_core::SigningParams;

fn main() {
    // Signing parameters have no Default: no decimals are ever assumed for a signer (0125).
    let _ = SigningParams::default();
}
