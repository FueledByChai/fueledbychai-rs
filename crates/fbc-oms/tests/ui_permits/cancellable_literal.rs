// A Cancellable permit is given only by the registry, never for a terminal order nor for
// another namespace's (I4): no literal builds one around a record.
use fbc_oms::{Cancellable, OrderRecord};

fn forge(rec: &mut OrderRecord) -> Cancellable<'_> {
    Cancellable { rec }
}

fn main() {}
