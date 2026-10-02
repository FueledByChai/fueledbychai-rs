use fbc_core::time::{KernelRxNs, WallNs};

fn main() {
    let _ = WallNs(5) - KernelRxNs(3);
}
