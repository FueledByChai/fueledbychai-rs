use fbc_core::Fee;

fn flip(fee: Fee) {
    // A fee has no negation; pnl() is the signed view for P&L.
    let _ = -fee;
}

fn main() {}
