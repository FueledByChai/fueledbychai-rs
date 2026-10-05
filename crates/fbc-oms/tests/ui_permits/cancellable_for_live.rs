// A Cancellable permit is not a Live one.
use fbc_oms::{Cancellable, Live};

fn needs_live(_: Live<'_>) {}

fn pass(permit: Cancellable<'_>) {
    needs_live(permit);
}

fn main() {}
