use fbc_core::{ExecCaps, OrderCaps};

// A venue that places orders states its fill capabilities in the same block: an exec block
// with order capabilities and no fill capabilities does not compile (decisions 0003, 0015).
fn exec(order: OrderCaps) -> ExecCaps {
    ExecCaps { order }
}

fn main() {
    let _ = exec;
}
