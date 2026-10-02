use fbc_core::time::{ExchNs, MonoNs};

fn main() {
    let _ = ExchNs(5) - MonoNs(3);
}
