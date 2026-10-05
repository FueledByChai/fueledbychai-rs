// What a permit builds cannot be forged from a command built by hand.
use fbc_core::{CancelOrder, VenueCommand};
use fbc_oms::PermittedCommand;

fn forge(cancel: CancelOrder) -> PermittedCommand {
    PermittedCommand {
        cmd: VenueCommand::Cancel(cancel),
    }
}

fn main() {}
