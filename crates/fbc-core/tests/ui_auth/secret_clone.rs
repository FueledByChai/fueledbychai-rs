// A credential exists once: `Secret` has no `Clone`, so no copy can drift into a log (0009).
use fbc_core::Secret;

fn main() {
    let secret = Secret::new(String::from("SYNTHETIC"));
    let _copy = secret.clone();
}
