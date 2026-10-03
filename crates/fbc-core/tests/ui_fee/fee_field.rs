use fbc_core::Fee;

fn rewrite(mut fee: Fee) {
    // The wrapped Money is private: a fee cannot be edited into another sign.
    fee.0.nanos = 1;
}

fn main() {}
