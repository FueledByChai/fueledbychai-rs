use fbc_core::OrderShapes;

fn main() {
    // The order shapes a venue lists for an instrument have no Default: no list nobody read is
    // ever taken to admit an order, nor to admit none (0136).
    let _ = OrderShapes::default();
}
