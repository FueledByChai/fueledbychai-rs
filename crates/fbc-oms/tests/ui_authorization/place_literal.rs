// Code outside fbc-oms cannot write an authorization for a place as a literal: its fields are
// private, so no command reaches a gateway around the OMS's caps and kill switch (0013 rule 2).
use fbc_core::{AccountKey, NewOrder, VenueCommand};
use fbc_oms::Authorization;

fn forge(acct: AccountKey, order: NewOrder, other: &Authorization) -> Authorization {
    Authorization {
        acct,
        market: order.inst,
        generation: other.generation(),
        cmd: VenueCommand::Place(order),
    }
}

fn main() {}
