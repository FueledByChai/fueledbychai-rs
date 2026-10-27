//! What `fixtures/hibachi/conformance` assumes: the spec table discovery reads from
//! `fixtures/hibachi/rest/exchange_info.json` (ETH/USDT-P, SOL/USDT-P, SEI/USDT-P) and the tests'
//! configuration (an orderbook depth of 3). Hibachi takes no credential and has no order entry.

use fbc_conformance::suite::Setup;
use fbc_core::{Secrets, StreamId};

/// The fixture directory, as the suite reads it.
pub const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../fixtures/hibachi/conformance"
);

/// What the fixtures assume.
pub fn assumed() -> Setup {
    Setup {
        specs: super::common::specs(),
        cfg: super::common::config(),
        creds: Secrets::new(),
        goldens: Vec::new(),
        exec_stream: StreamId(0),
        order_entry: None,
        bootstrap: None,
    }
}
