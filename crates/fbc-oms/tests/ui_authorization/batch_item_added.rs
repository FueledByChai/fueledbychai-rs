// An authorization for a batch cannot gain an item the OMS did not check: its command is read
// only through a shared reference.
use fbc_core::{NewOrder, VenueCommand};
use fbc_oms::Authorization;

fn smuggle(auth: &mut Authorization, extra: NewOrder) {
    if let VenueCommand::PlaceBatch(items) = auth.command() {
        items.push(extra);
    }
}

fn main() {}
