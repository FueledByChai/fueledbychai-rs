use fbc_core::Fee;

fn magnitude(fee: Fee) {
    // A fee has no absolute value: its sign is the whole point.
    let _ = fee.abs();
}

fn main() {}
