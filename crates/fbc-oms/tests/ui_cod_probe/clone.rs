//! A probe declaration is neither `Clone` nor `Copy`: one declaration goes into one registry
//! (decision 0140 item 6).

use fbc_core::InstrumentId;
use fbc_oms::{CodProbeRun, Registry};

fn main() {
    let run = CodProbeRun::owner_supervised(InstrumentId::new(1));
    let first = Registry::new().for_cod_probe(run.clone());
    let second = Registry::new().for_cod_probe(run);
    let _ = (first, second);
}
