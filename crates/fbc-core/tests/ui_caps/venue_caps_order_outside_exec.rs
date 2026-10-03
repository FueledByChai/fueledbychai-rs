use fbc_core::{MatchingCaps, MdCaps, OrderCaps, Readiness, VenueCaps};

// Order capabilities live only inside the exec block, beside the fill capabilities: the
// design's separate `order` field, which allowed orders with no fill capabilities, is gone
// (decision 0015).
fn caps(order: OrderCaps, matching: MatchingCaps, md: MdCaps) -> VenueCaps {
    VenueCaps {
        order: Some(order),
        matching,
        md,
        limits: vec![],
        readiness_ceiling: Readiness::Live,
    }
}

fn main() {
    let _ = caps;
}
