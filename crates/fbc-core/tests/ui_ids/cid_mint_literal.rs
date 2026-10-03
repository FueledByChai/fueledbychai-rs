use fbc_core::{CidMint, Namespace};

fn main() {
    let _ = Namespace::new(1);
    let mut mint = CidMint { last: 0 };
    let _ = mint.mint();
}
