use fbc_core::{ClientIdFormat, Namespace, VenueFeeSign, dispatch};

fn main() {
    let fmt = ClientIdFormat::Uuid;
    // The scope is lent to the callback only; it cannot be kept to build venue ids later.
    let scope = dispatch(&fmt, Namespace::new(1), VenueFeeSign::PositiveIsCost, |scope| scope);
    let _ = scope.venue_order_id("1");
}
