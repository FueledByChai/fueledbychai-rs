//! A probe declaration is made only by `CodProbeRun::owner_supervised` (decision 0140 item 6).

use fbc_core::InstrumentId;
use fbc_oms::{CodProbeRun, Registry};

fn main() {
    let run = CodProbeRun {
        market: InstrumentId::new(1),
    };
    let _ = Registry::new().for_cod_probe(run);
}
