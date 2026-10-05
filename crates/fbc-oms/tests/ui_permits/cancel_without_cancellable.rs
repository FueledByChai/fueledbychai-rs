// A cancel is built only through a Cancellable permit: a record is not one.
use fbc_core::OrderCaps;
use fbc_oms::{Cancellable, Registry};

fn cancel(reg: &mut Registry, cid: fbc_core::ClientOrderId, caps: &OrderCaps) {
    let _ = Cancellable::cancel(reg.get(cid).unwrap(), caps);
}

fn main() {}
