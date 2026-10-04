use fbc_core::{Namespace, VenueCaps, dispatch, dispatch_market_data};

fn escape(caps: &VenueCaps) {
    // The scope is lent to the callback only; it cannot be kept to build venue ids later, from
    // either dispatch.
    let scope = dispatch(caps, Namespace::new(1), |scope| scope);
    let _ = scope.venue_order_id("1");
    let scope = dispatch_market_data(caps, |scope| scope);
    let _ = scope.venue_order_id("1");
}

fn main() {}
