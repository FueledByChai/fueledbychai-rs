use fbc_core::time::{KernelRxNs, MonoNs};

fn main() {
    let _ = MonoNs(5) - KernelRxNs(3);
}
