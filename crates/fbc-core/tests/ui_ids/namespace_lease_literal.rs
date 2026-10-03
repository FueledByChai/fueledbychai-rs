use fbc_core::{AccountKey, CidMint, Namespace, NamespaceLease, WallNs};

fn main() {
    // A lease comes only from NamespaceLease::acquire, which takes the file lock.
    let lease = NamespaceLease { account: AccountKey::new(1), ns: Namespace::new(1) };
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let _ = mint.mint();
}
