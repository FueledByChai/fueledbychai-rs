use fbc_core::time::{KernelRxNs, WallNs};

fn main() {
    let _ = KernelRxNs(5) - WallNs(3);
}
