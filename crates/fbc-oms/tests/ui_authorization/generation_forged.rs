// A state generation cannot be made up outside fbc-oms, so an authorization cannot be dated
// to a generation its market never had.
use fbc_oms::StateGeneration;

fn current() -> StateGeneration {
    StateGeneration(u64::MAX)
}

fn main() {}
