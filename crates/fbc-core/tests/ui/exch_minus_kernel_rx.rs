use fbc_core::time::{ExchNs, KernelRxNs};

fn main() {
    let _ = ExchNs(5) - KernelRxNs(3);
}
