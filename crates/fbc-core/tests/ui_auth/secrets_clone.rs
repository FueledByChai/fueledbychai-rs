// A set of credentials exists once: `Secrets` has no `Clone` (0009).
use fbc_core::Secrets;

fn main() {
    let secrets = Secrets::new();
    let _copy = secrets.clone();
}
