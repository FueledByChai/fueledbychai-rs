//! A cancel-many reaches an order-entry session only as an fbc-oms authorization: neither of
//! the session's submissions takes it as a plain command (decisions 0045, 0057).

use fbc_core::{CancelOrder, VenueCommand};
use fbc_runtime::ExecOrders;

fn submit(orders: &ExecOrders, item: CancelOrder) {
    let _ = orders.submit(VenueCommand::CancelMany(vec![item.clone()]));
    let _ = orders.submit_control(VenueCommand::CancelMany(vec![item]));
}

fn main() {}
