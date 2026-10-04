use fbc_core::{DecodeScope, VenueCaps};

fn forge(caps: &VenueCaps) {
    let scope = DecodeScope { caps, own: None };
    let _ = scope.venue_order_id("1");
}

fn main() {}
