use fbc_core::time::{ExchNs, KernelRxNs};

fn main() {
    let _ = KernelRxNs(5) - ExchNs(3);
}
