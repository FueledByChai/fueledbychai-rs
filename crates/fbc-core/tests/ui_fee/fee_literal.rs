use fbc_core::{AssetSym, Fee, Money};

fn main() {
    // A Fee is not built from a Money: only DecodeScope::fee makes one.
    let _ = Fee(Money::new(1, AssetSym::new("USDC").unwrap()));
}
