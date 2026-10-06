//! A cancel reaches an order-entry session only as an fbc-oms authorization: neither of the
//! session's submissions takes it as a plain command (decisions 0045, 0057).

use fbc_core::{CancelOrder, VenueCommand};
use fbc_runtime::ExecOrders;

fn submit(orders: &ExecOrders, cmd: CancelOrder) {
    let _ = orders.submit(VenueCommand::Cancel(cmd.clone()));
    let _ = orders.submit_control(VenueCommand::Cancel(cmd.clone()));
}

fn main() {}
