//! An instrument cancel-all reaches an order-entry session only as an fbc-oms authorization: neither of the
//! session's submissions takes it as a plain command (decisions 0045, 0057).

use fbc_core::{CancelScope, VenueCommand};
use fbc_runtime::ExecOrders;

fn submit(orders: &ExecOrders, cmd: CancelScope) {
    let _ = orders.submit(VenueCommand::CancelAll(cmd));
    let _ = orders.submit_control(VenueCommand::CancelAll(cmd));
}

fn main() {}
