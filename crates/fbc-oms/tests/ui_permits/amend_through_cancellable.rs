// A Cancellable permit (any order that is not terminal, PendingNew and Unknown included)
// builds a cancel, never an amend.
use fbc_core::{Lots, OrderCaps, Ticks};
use fbc_oms::Registry;

fn amend(reg: &mut Registry, cid: fbc_core::ClientOrderId, caps: &OrderCaps, qty: Lots) {
    let _ = reg.cancellable(cid).unwrap().amend(caps, Ticks(1), qty);
}

fn main() {}
