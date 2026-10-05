use fbc_core::{AmendAck, AmendCaps, AmendQty};

fn main() {
    // An amend capability that does not say which references an amend can name does not
    // compile: no reference kind is assumed (decision 0003, FBC-6c9).
    let _ = AmendCaps {
        price: true,
        qty: true,
        flags: false,
        when_partially_filled: false,
        reject_keeps_original: true,
        keeps_venue_id: true,
        ack: AmendAck::ReplacedEvent,
        qty_semantics: AmendQty::TotalIncludingFilled,
        keeps_priority: None,
    };
}
