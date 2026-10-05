// A Live permit is given only by the registry, for a resting order with nothing in flight: no
// literal builds one around a record, which it holds mutably while the amend is built.
use fbc_oms::{Live, OrderRecord};

fn forge(rec: &mut OrderRecord) -> Live<'_> {
    Live { rec }
}

fn main() {}
