// A Live permit builds an amend; a cancel is built through a Cancellable one.
use fbc_core::OrderCaps;
use fbc_oms::Registry;

fn cancel(reg: &Registry, cid: fbc_core::ClientOrderId, caps: &OrderCaps) {
    let _ = reg.live(cid).unwrap().cancel(caps);
}

fn main() {}
