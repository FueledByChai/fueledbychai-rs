// An authorization cannot be cloned: each one is spent once, on the submit that consumes it.
use fbc_oms::Authorization;

fn twice(auth: &Authorization) -> (Authorization, Authorization) {
    (auth.clone(), Clone::clone(auth))
}

fn main() {}
