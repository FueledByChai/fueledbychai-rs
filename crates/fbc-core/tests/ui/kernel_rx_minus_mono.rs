use fbc_core::time::{KernelRxNs, MonoNs};

fn main() {
    let _ = KernelRxNs(5) - MonoNs(3);
}
