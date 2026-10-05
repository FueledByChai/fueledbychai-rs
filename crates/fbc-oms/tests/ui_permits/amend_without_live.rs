// An amend is built only through a Live permit: a record is not one.
use fbc_core::{Lots, OrderCaps, Ticks};
use fbc_oms::{Live, Registry};

fn amend(reg: &Registry, cid: fbc_core::ClientOrderId, caps: &OrderCaps, qty: Lots) {
    let _ = Live::amend(reg.get(cid).unwrap(), caps, Ticks(1), qty, false);
}

fn main() {}
