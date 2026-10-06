//! A batch of places, and so each of its items, reaches an order-entry session only as an
//! fbc-oms authorization: neither of the session's submissions takes it as a plain command
//! (decisions 0045, 0057).

use fbc_core::{NewOrder, VenueCommand};
use fbc_runtime::ExecOrders;

fn submit(orders: &ExecOrders, item: NewOrder) {
    let _ = orders.submit(VenueCommand::PlaceBatch(vec![item.clone()]));
    let _ = orders.submit_control(VenueCommand::PlaceBatch(vec![item]));
}

fn main() {}
