//! An amend reaches an order-entry session only as an fbc-oms authorization: neither of the
//! session's submissions takes it as a plain command (decisions 0045, 0057).

use fbc_core::{AmendOrder, VenueCommand};
use fbc_runtime::ExecOrders;

fn submit(orders: &ExecOrders, cmd: AmendOrder) {
    let _ = orders.submit(VenueCommand::Amend(cmd.clone()));
    let _ = orders.submit_control(VenueCommand::Amend(cmd.clone()));
}

fn main() {}
