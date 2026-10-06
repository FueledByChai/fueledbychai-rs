//! A place reaches an order-entry session only as an fbc-oms authorization: neither of the
//! session's submissions takes it as a plain command (decisions 0045, 0057).

use fbc_core::{NewOrder, VenueCommand};
use fbc_runtime::ExecOrders;

fn submit(orders: &ExecOrders, cmd: NewOrder) {
    let _ = orders.submit(VenueCommand::Place(cmd.clone()));
    let _ = orders.submit_control(VenueCommand::Place(cmd.clone()));
}

fn main() {}
