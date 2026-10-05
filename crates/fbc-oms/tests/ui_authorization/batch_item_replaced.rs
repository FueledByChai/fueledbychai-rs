// Nor can its command be swapped for a batch the OMS did not check: its fields are private.
use fbc_core::{NewOrder, VenueCommand};
use fbc_oms::Authorization;

fn smuggle(auth: &mut Authorization, extra: NewOrder) {
    auth.cmd = VenueCommand::PlaceBatch(vec![extra]);
}

fn main() {}
