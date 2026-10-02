use fbc_core::time::{ExchNs, WallNs};

fn main() {
    let _ = WallNs(5) - ExchNs(3);
}
