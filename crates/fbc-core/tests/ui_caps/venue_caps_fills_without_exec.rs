use fbc_core::{FillCaps, MatchingCaps, MdCaps, Readiness, VenueCaps};

// A market-data-only venue (no exec block) cannot claim a fill source or a fee sign: fill
// capabilities exist only inside the exec block (decision 0015).
fn caps(fills: FillCaps, matching: MatchingCaps, md: MdCaps) -> VenueCaps {
    VenueCaps {
        exec: None,
        fills,
        matching,
        md,
        limits: vec![],
        exec_limits: None,
        readiness_ceiling: Readiness::Record,
    }
}

fn main() {
    let _ = caps;
}
