// An authorization the OMS issued for one cancel cannot be turned into a cancel-all, nor be
// given another market's generation.
use fbc_core::{CancelScope, InstrumentId, VenueCommand};
use fbc_oms::Authorization;

fn widen(mut auth: Authorization, other: &Authorization, inst: InstrumentId) -> Authorization {
    auth.cmd = VenueCommand::CancelAll(CancelScope::Instrument(inst));
    auth.generation = other.generation();
    auth
}

fn main() {}
