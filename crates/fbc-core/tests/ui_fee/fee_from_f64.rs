use fbc_core::Fee;

fn main() {
    // No conversion from a float.
    let _: Fee = 0.0125f64.into();
}
