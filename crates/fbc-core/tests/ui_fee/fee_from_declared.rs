use fbc_core::{AssetSym, Fee, VenueFeeSign};

fn main() {
    // The constructor DecodeScope::fee uses is private to fbc-core.
    let _ = Fee::from_declared(1, AssetSym::new("USDC").unwrap(), VenueFeeSign::PositiveIsCost);
}
