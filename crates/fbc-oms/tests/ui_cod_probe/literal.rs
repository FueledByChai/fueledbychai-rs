//! A probe declaration is made only by `CodProbeRun::owner_supervised` or
//! `CodProbeRun::owner_supervised_places` (decisions 0140 item 6, 0148 item 3).

use fbc_core::InstrumentId;
use fbc_oms::{CodProbeRun, Registry};

fn main() {
    let run = CodProbeRun {
        market: InstrumentId::new(1),
        flat_start: false,
    };
    let _ = Registry::new().for_cod_probe(run);
}
