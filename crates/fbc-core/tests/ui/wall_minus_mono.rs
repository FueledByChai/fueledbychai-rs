use fbc_core::time::{MonoNs, WallNs};

fn main() {
    let _ = WallNs(5) - MonoNs(3);
}
