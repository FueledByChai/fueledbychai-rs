// An encode receipt reports the nonces an encode took from its EncodeCtx, and nothing else:
// it has no public fields, so a codec cannot report a nonce the context did not reserve.
use fbc_core::EncodeReceipt;

fn main() {
    let _forged = EncodeReceipt {
        nonces: vec![(0, 42)],
    };
}
