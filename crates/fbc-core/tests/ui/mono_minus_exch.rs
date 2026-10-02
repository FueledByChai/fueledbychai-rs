use fbc_core::time::{ExchNs, MonoNs};

fn main() {
    let _ = MonoNs(5) - ExchNs(3);
}
