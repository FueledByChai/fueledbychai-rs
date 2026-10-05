// Code outside fbc-oms cannot call the crate-private constructor the OMS issues an
// authorization with, here for an amend.
use fbc_core::{AccountKey, AmendOrder, VenueCommand};
use fbc_oms::Authorization;

fn forge(acct: AccountKey, amend: AmendOrder) -> Authorization {
    Authorization::issue(acct, VenueCommand::Amend(amend), &Default::default()).unwrap()
}

fn main() {}
