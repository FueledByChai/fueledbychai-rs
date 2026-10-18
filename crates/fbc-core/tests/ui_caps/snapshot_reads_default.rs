use fbc_core::SnapshotReads;

fn main() {
    // How a resync reads the account has no Default: a venue states it (decisions 0003, 0095).
    let _ = SnapshotReads::default();
}
