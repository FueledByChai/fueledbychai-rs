use fbc_core::{CidMint, Namespace, WallNs};

fn main() {
    // A mint needs a held NamespaceLease; a bare namespace is not one.
    let mut mint = CidMint::new(Namespace::new(1), 0, 0, WallNs(0));
    let _ = mint.mint();
}
