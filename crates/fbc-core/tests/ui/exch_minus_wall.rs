use fbc_core::time::{ExchNs, WallNs};

fn main() {
    let _ = ExchNs(5) - WallNs(3);
}
