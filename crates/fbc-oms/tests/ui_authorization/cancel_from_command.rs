// A cancel is order-affecting too: no conversion turns a command into an authorization.
use fbc_core::{CancelOrder, VenueCommand};
use fbc_oms::Authorization;

fn forge(cancel: CancelOrder) -> Authorization {
    VenueCommand::Cancel(cancel).into()
}

fn main() {}
