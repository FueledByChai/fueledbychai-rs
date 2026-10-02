use fbc_core::time::{MonoNs, WallNs};

fn main() {
    let _ = MonoNs(5) - WallNs(3);
}
