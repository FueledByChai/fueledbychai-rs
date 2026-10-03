// Decision 0005: a venue not knowing an order (RejectKind::NotFound) moves it to Unknown and is
// never terminal, so an order state cannot say the order was rejected as not found.
use fbc_core::{RejectKind, TerminalReject, VenueOrderState};

fn main() {
    let _state = VenueOrderState::Rejected(RejectKind::NotFound);
    let _literal = TerminalReject(RejectKind::NotFound);
}
